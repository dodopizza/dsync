# Cosmos DB deletes emulation

As a work around the Cosmos DB changestream [limitation](https://learn.microsoft.com/en-us/azure/cosmos-db/mongodb/change-streams?tabs=javascript#current-limitations), dsync implements a specially-designed algorithm that detects deletes on the source and emits them on a periodic basis.

## Algorithm

- We use an external "Witness" index to track which documents we've already seen coming from the source.
    - The key assumption is that the set of keys in the Witness index is always a superset of the keys in the Source index (W = S + whatever was deleted)
    - For simplicity, we can use the destination's _id index directly as a Witness as it already has exactly what we need maintained with transactional semantics.
 - At any point in time, any document that is present in the Witness index but absent from the source is considered deleted on the source.

1. Take the namespaces the stream is running on.
2. For every namespace, walk all Witness keys once, in `_id` order, handed out in pages of 1000. Counts are not compared: a delete plus an insert keeps them equal. A key is a tuple: `_id`, then the namespace's shard key fields in order when `--full-document-key` is on (otherwise `_id` alone). A document that lacks a shard key field has `null` in that position, on the Witness and on the Source alike. Tuples with an equal `_id` are never split across pages.
3. Check each page against the Source with one `find` on `{_id: {$in: distinct _ids of the page}}` (projection `_id` plus the shard key fields) and take the set difference of tuples on the client. Components are compared by value for numeric types (int32, int64, integral double) and by BSON type plus bytes otherwise; components are length-prefixed so that ("a", "bc") and ("ab", "c") differ. Tuples absent from the result become delete messages, sent into the data channel with the Witness tuple's exact bytes: the `_id` part first, then one part per shard key field, named after the field, like change stream events.
4. Any error aborts the whole pass without emitting deletes, so a throttled or timed-out read is never taken for a missing document; a pass error ends the stream, and a restart resumes from the checkpoint. A source document whose `_id` matches no `_id` of the page (the server matched across types the client keeps apart, e.g. Decimal128 5 and int 5) aborts the pass as well, instead of deleting a live document. When the `_id` is known but the document's tuple is not among the page's tuples (the document exists in the source under a shard key value the Witness lacks, e.g. it was re-created under another value and the stream has not applied it yet, or the value does not round-trip between source and Witness), every Witness tuple of the page with that `_id` is undecided: none of them is deleted in this pass, they are left for a later pass and counted as deferred. A deferred count that stays non-zero means the shard key value does not round-trip between source and Witness.
5. After the deletes of a pass are sent, one line logs the number of namespaces walked, keys scanned, deletes emitted and keys deferred.

The Witness is an interface (`Witness` in `connectors/cosmos/witness.go`): scan a namespace's key tuples given its shard key fields, emitting pages. Contract: keys of one namespace arrive sorted by `_id`, so tuples with an equal `_id` are adjacent (equality is by value for numeric ids; ids equal only across other types, e.g. int and Decimal128, fall under the abort of step 4); the pass re-chunks pages so that such a run is never split. The MongoDB witness runs one sorted `find` per namespace, projecting `_id` and the shard key fields (dotted paths are followed into subdocuments), and is the only implementation so far.

## Pros: 
- For users, there's no need to change the application code to use soft deletes.
- The algorithm works reliably and ensures eventual consistency.
- The behavior can be turned on/off anytime in the process without restarting the sync from scratch.

## Cons:
- Index scans can be expensive hence the check period may need to be adjusted to balance the lag and the responsiveness.