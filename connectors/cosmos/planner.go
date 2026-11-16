/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package cosmos

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/adiom-data/dsync/protocol/iface"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"golang.org/x/exp/rand"
)

var (
	// System databases that we don't want to copy
	ExcludedDBListForIC = []string{"local", "config", "admin", "adiom-internal"}
	// System collections that we don't want to copy (regex pattern)
	ExcludedSystemCollPattern = "^system[.]"
)

// function to shuffle the tasks
func shuffleTasks(tasks []iface.ReadPlanTask) {
	for i := range tasks {
		j := rand.Intn(i + 1)
		tasks[i], tasks[j] = tasks[j], tasks[i]
	}
}

type planner struct {
	Ctx      context.Context
	Client   *mongo.Client
	settings ConnectorSettings
}

// partitionTasksIfNecessary checks all the namespace tasks and partitions them if necessary
// returns the final list of tasks to be executed with unique task ids
func (cc *planner) partitionTasksIfNecessary(namespaceTasks []iface.Namespace) []iface.ReadPlanTask {
	countCheckChannel := make(chan iface.Namespace, len(namespaceTasks))

	finalTasksChannel := make(chan iface.ReadPlanTask, len(namespaceTasks))
	finalTasks := make([]iface.ReadPlanTask, 0, len(namespaceTasks))

	// add namespace tasks to the queue to check their counts
	go func() {
		for _, task := range namespaceTasks {
			countCheckChannel <- task
		}
		close(countCheckChannel)
	}()

	go cc.parallelNamespaceTaskPreparer(countCheckChannel, finalTasksChannel)

	// get all the finalized tasks and assign sequential task ids
	taskId := iface.ReadPlanTaskID(1)
	for task := range finalTasksChannel {
		task.Id = taskId
		finalTasks = append(finalTasks, task)
		taskId++
	}
	return finalTasks
}

func (cc *planner) createReadPlanTaskForNs(ns iface.Namespace) iface.ReadPlanTask {
	task := iface.ReadPlanTask{}
	task.Def.Db = ns.Db
	task.Def.Col = ns.Col
	task.DocsCopied = 0

	return task
}

// In parallel checks the count of the namespace and sends the resulting tasks to the final channel, closing it when done
func (cc *planner) parallelNamespaceTaskPreparer(countCheckChannel <-chan iface.Namespace, finalTasksChannel chan<- iface.ReadPlanTask) {
	//define workgroup
	wg := sync.WaitGroup{}
	wg.Add(cc.settings.NumParallelPartitionWorkers)
	// create workers to do the counting
	for i := 0; i < cc.settings.NumParallelPartitionWorkers; i++ {
		go func() {
			for nsTask := range countCheckChannel {
				collection := cc.Client.Database(nsTask.Db).Collection(nsTask.Col)
				count, err := collection.EstimatedDocumentCount(cc.Ctx)
				slog.Debug(fmt.Sprintf("Count for task %v: %v", nsToString(nsTask), count))

				if err != nil {
					if cc.Ctx.Err() == context.Canceled {
						slog.Debug(fmt.Sprintf("Count error: %v, but the context was cancelled", err))
					} else {
						slog.Warn(fmt.Sprintf("Failed to count documents, not splitting: %v", err))
						task := cc.createReadPlanTaskForNs(nsTask)
						task.EstimatedDocCount = count
						finalTasksChannel <- task
						continue
					}
				}

				if count < cc.settings.TargetDocCountPerPartition*2 { //not worth doing anything
					task := cc.createReadPlanTaskForNs(nsTask)
					task.EstimatedDocCount = count
					finalTasksChannel <- task
				} else { //we need to split it
					slog.Debug(fmt.Sprintf("Need to split task %v with count %v", nsTask, count))
					//get min and max bounds
					min, max, err := getMinAndMax(cc.Ctx, cc.Client, nsTask, cc.settings.partitionKey)
					if err != nil {
						slog.Warn(fmt.Sprintf("Failed to get min and max boundaries for a task %v so not splitting: %v", nsTask, err))
						task := cc.createReadPlanTaskForNs(nsTask)
						task.EstimatedDocCount = count
						finalTasksChannel <- task
						continue
					}
					slog.Debug(fmt.Sprintf("Min and max boundaries for task %v: %v, %v", nsTask, min, max))
					// find approximate split points
					numParts := int(count / cc.settings.TargetDocCountPerPartition)
					approxBounds, err := splitRange(min, max, numParts)
					if err != nil {
						slog.Warn(fmt.Sprintf("Failed to split range for task %v so not splitting: %v", nsTask, err))
						task := cc.createReadPlanTaskForNs(nsTask)
						task.EstimatedDocCount = count
						finalTasksChannel <- task
						continue
					}
					slog.Debug(fmt.Sprintf("Number of approximate split points for task %v: %v", nsTask, len(approxBounds)))

					//send min and max tasks to finalized channel
					minTask := cc.createReadPlanTaskForNs(nsTask)
					minTask.Def.PartitionKey = cc.settings.partitionKey
					minTask.Def.High = min        //only setting the high boundary indicating that we want (-INF, min)
					minTask.EstimatedDocCount = 0 //we don't expect anything to be there

					maxTask := cc.createReadPlanTaskForNs(nsTask)
					maxTask.Def.PartitionKey = cc.settings.partitionKey
					maxTask.Def.Low = max         //only setting the low boundary indicating that we want (max, INF)
					maxTask.EstimatedDocCount = 0 //we don't expect anything to be there

					finalTasksChannel <- minTask
					finalTasksChannel <- maxTask

					//send the tasks between the split points
					for i := 0; i < len(approxBounds)-1; i++ {
						task := cc.createReadPlanTaskForNs(nsTask)
						task.Def.PartitionKey = cc.settings.partitionKey
						task.Def.Low = approxBounds[i]
						task.Def.High = approxBounds[i+1]
						task.EstimatedDocCount = cc.settings.TargetDocCountPerPartition
						finalTasksChannel <- task
					}

				}
			}
			wg.Done()
		}()
	}

	wg.Wait()
	close(finalTasksChannel)
}
