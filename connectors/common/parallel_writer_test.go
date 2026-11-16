package common

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	pwWorkers   = 4
	pwNoFireFor = 50 * time.Millisecond
	pwTimeout   = 5 * time.Second
	pwJoinBound = time.Second
)

var (
	errPWInjected    = errors.New("injected failure")
	errPWJoinTimeout = errors.New("workers did not stop in time")
)

type pwFired struct {
	msg  iface.DataMessage
	seen map[string]bool
}

type pwGate struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	err     error
}

func (g *pwGate) open() { g.once.Do(func() { close(g.release) }) }

type pwConn struct {
	mu         sync.Mutex
	written    map[string]bool
	fired      chan pwFired
	failed     chan struct{}
	gates      map[string]*pwGate
	failKeys   map[string]error
	barrierErr error
	handled    []error
}

func newPWConn(holdKeys ...string) *pwConn {
	c := &pwConn{
		written:  map[string]bool{},
		fired:    make(chan pwFired, 16),
		failed:   make(chan struct{}, 8),
		gates:    map[string]*pwGate{},
		failKeys: map[string]error{},
	}
	for _, k := range holdKeys {
		c.gates[k] = &pwGate{entered: make(chan struct{}, 1), release: make(chan struct{})}
	}
	return c
}

func (c *pwConn) open() {
	for _, g := range c.gates {
		g.open()
	}
}

func (c *pwConn) has(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.written[key]
}

func (c *pwConn) handledErrs() []error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]error(nil), c.handled...)
}

func pwKey(m iface.DataMessage) string {
	if len(m.Id) > 0 {
		return string(m.Id[0].Data)
	}
	return m.Loc
}

func (c *pwConn) ProcessDataMessages(msgs []iface.DataMessage) error {
	for _, m := range msgs {
		if g := c.gates[pwKey(m)]; g != nil {
			g.entered <- struct{}{}
			<-g.release
			if g.err != nil {
				return g.err
			}
		}
		if err := c.failKeys[pwKey(m)]; err != nil {
			return err
		}
	}
	c.mu.Lock()
	for _, m := range msgs {
		c.written[pwKey(m)] = true
	}
	c.mu.Unlock()
	return nil
}

func (c *pwConn) HandleBarrierMessage(m iface.DataMessage) error {
	c.mu.Lock()
	seen := map[string]bool{}
	for k := range c.written {
		seen[k] = true
	}
	c.mu.Unlock()
	c.fired <- pwFired{m, seen}
	return c.barrierErr
}

func (c *pwConn) HandlerError(err error) error {
	c.mu.Lock()
	c.handled = append(c.handled, err)
	c.mu.Unlock()
	c.failed <- struct{}{}
	return err
}

type pwHarness struct {
	pw       *ParallelWriter
	cancel   context.CancelFunc
	stopOnce sync.Once
	stopErr  error
}

func startPW(t *testing.T, c *pwConn, multi bool) *pwHarness {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	h := &pwHarness{pw: NewParallelWriter(ctx, c, pwWorkers, 10, multi), cancel: cancel}
	h.pw.Start()
	t.Cleanup(func() {
		c.open()
		if err := h.stop(); errors.Is(err, errPWJoinTimeout) {
			t.Errorf("cleanup: %v", err)
		}
		cancel()
	})
	return h
}

func (h *pwHarness) stop() error {
	h.stopOnce.Do(func() {
		done := make(chan error, 1)
		go func() { done <- h.pw.StopAndWait() }()
		select {
		case h.stopErr = <-done:
			return
		case <-time.After(pwJoinBound):
		}
		h.cancel()
		select {
		case <-done:
			h.stopErr = errPWJoinTimeout
		case <-time.After(pwJoinBound):
			h.stopErr = fmt.Errorf("%w: still running after cancel", errPWJoinTimeout)
		}
	})
	return h.stopErr
}

func pwRecv[T any](t *testing.T, ch <-chan T, label string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(pwTimeout):
		t.Fatalf("timed out waiting for %s", label)
	}
	var zero T
	return zero
}

func pwAssertNoFire(t *testing.T, c *pwConn) {
	t.Helper()
	select {
	case f := <-c.fired:
		t.Fatalf("barrier fired early: type %d token %q", f.msg.BarrierType, f.msg.BarrierCdcResumeToken)
	case <-time.After(pwNoFireFor):
	}
}

func pwUpdate(i int, ns int) iface.DataMessage {
	return iface.DataMessage{
		MutationType: iface.MutationType_Update,
		Loc:          fmt.Sprintf("ns%d", ns),
		Id:           []*adiomv1.BsonValue{{Data: fmt.Appendf(nil, "id-%d", i)}},
	}
}

func pwUpdatesPerWorker() [][]iface.DataMessage {
	out := make([][]iface.DataMessage, pwWorkers)
	for i := 0; i < 1000; i++ {
		w := hashDataMsgId(pwUpdate(i, 0)) % pwWorkers
		if len(out[w]) < 3 {
			out[w] = append(out[w], pwUpdate(i, min(len(out[w]), 2)/2))
		}
	}
	return out
}

func pwScheduleAll(t *testing.T, h *pwHarness, per [][]iface.DataMessage, skip iface.DataMessage) []string {
	t.Helper()
	var keys []string
	for _, ms := range per {
		for _, m := range ms {
			keys = append(keys, pwKey(m))
			if pwKey(m) != pwKey(skip) {
				require.NoError(t, h.pw.ScheduleDataMessage(m))
			}
		}
	}
	return keys
}

func pwInsertBatch(i int) iface.DataMessage {
	return iface.DataMessage{
		MutationType: iface.MutationType_InsertBatch,
		Loc:          fmt.Sprintf("b%d", i),
		DataBatch:    [][]byte{{1}},
	}
}

func pwTaskBarrier(id uint) iface.DataMessage {
	return iface.DataMessage{MutationType: iface.MutationType_Barrier, BarrierType: iface.BarrierType_TaskComplete, BarrierTaskId: id}
}

func pwCDCBarrier(token string) iface.DataMessage {
	return iface.DataMessage{MutationType: iface.MutationType_Barrier, BarrierType: iface.BarrierType_CdcResumeTokenUpdate, BarrierCdcResumeToken: []byte(token)}
}

func pwBlockBarrier() iface.DataMessage {
	return iface.DataMessage{MutationType: iface.MutationType_Barrier, BarrierType: iface.BarrierType_Block}
}

var pwKinds = map[string]iface.DataMessage{"task": pwTaskBarrier(3), "cdc": pwCDCBarrier("tok")}

func pwModes(t *testing.T, fn func(t *testing.T, multi bool)) {
	for _, multi := range []bool{false, true} {
		t.Run(fmt.Sprintf("multi=%v", multi), func(t *testing.T) { fn(t, multi) })
	}
}

func pwCases(t *testing.T, fn func(t *testing.T, barrier iface.DataMessage, multi bool)) {
	for name, barrier := range pwKinds {
		t.Run(name, func(t *testing.T) {
			pwModes(t, func(t *testing.T, multi bool) { fn(t, barrier, multi) })
		})
	}
}

func pwWaitQueued(t *testing.T, h *pwHarness, worker int, n int) {
	t.Helper()
	deadline := time.Now().Add(pwTimeout)
	for len(h.pw.workers[worker].queue) < n {
		if time.Now().After(deadline) {
			t.Fatalf("worker %d queue never reached %d messages", worker, n)
		}
		runtime.Gosched()
	}
}

func pwHoldPrimer(t *testing.T, h *pwHarness, c *pwConn, primer iface.DataMessage) {
	t.Helper()
	require.NoError(t, h.pw.ScheduleDataMessage(primer))
	pwRecv(t, c.gates[pwKey(primer)].entered, "primer write entered")
}

func TestParallelWriterBarrierWaitsForHeldWorkerAndFlushesBufferedWrites(t *testing.T) {
	pwCases(t, func(t *testing.T, barrier iface.DataMessage, multi bool) {
		per := pwUpdatesPerWorker()
		primer := per[0][0]
		c := newPWConn(pwKey(primer))
		h := startPW(t, c, multi)
		pwHoldPrimer(t, h, c, primer)
		keys := pwScheduleAll(t, h, per, primer)
		require.NoError(t, h.pw.ScheduleBarrier(barrier))
		pwAssertNoFire(t, c)
		c.open()
		f := pwRecv(t, c.fired, "barrier handler")
		for _, k := range keys {
			assert.True(t, f.seen[k], k)
		}
		assert.Equal(t, barrier.BarrierType, f.msg.BarrierType)
		assert.Equal(t, barrier.BarrierTaskId, f.msg.BarrierTaskId)
		assert.Equal(t, barrier.BarrierCdcResumeToken, f.msg.BarrierCdcResumeToken)
		require.NoError(t, h.stop())
		assert.Empty(t, c.fired)
	})
}

func TestParallelWriterFailedBufferedWriteNeverReleasesBarrier(t *testing.T) {
	pwCases(t, func(t *testing.T, barrier iface.DataMessage, multi bool) {
		per := pwUpdatesPerWorker()
		primer := per[0][0]
		c := newPWConn(pwKey(primer))
		c.failKeys[pwKey(per[0][2])] = errPWInjected
		h := startPW(t, c, multi)
		pwHoldPrimer(t, h, c, primer)
		pwScheduleAll(t, h, per, primer)
		require.NoError(t, h.pw.ScheduleBarrier(barrier))
		c.open()
		pwRecv(t, c.failed, "HandlerError")
		pwAssertNoFire(t, c)
		assert.ErrorIs(t, h.stop(), errPWInjected)
		assert.Empty(t, c.fired)
	})
}

func TestParallelWriterFailedWorkerNeverReleasesLaterBarrier(t *testing.T) {
	pwCases(t, func(t *testing.T, barrier iface.DataMessage, multi bool) {
		per := pwUpdatesPerWorker()
		failKey, laterKey := pwKey(per[0][0]), pwKey(per[1][0])
		c := newPWConn(failKey, laterKey)
		c.gates[failKey].err = errPWInjected
		h := startPW(t, c, multi)
		pwScheduleAll(t, h, per, iface.DataMessage{})
		pwRecv(t, c.gates[failKey].entered, "failing write entered")
		pwRecv(t, c.gates[laterKey].entered, "later write entered")
		require.NoError(t, h.pw.ScheduleBarrier(barrier))
		c.gates[failKey].open()
		pwRecv(t, c.failed, "HandlerError")
		c.gates[laterKey].open()
		pwAssertNoFire(t, c)
		assert.ErrorIs(t, h.stop(), errPWInjected)
		assert.Empty(t, c.fired)
		assert.Len(t, c.handledErrs(), 1)
	})
}

func TestParallelWriterTaskBarrierWaitsForHeldInsertBatch(t *testing.T) {
	pwModes(t, func(t *testing.T, multi bool) {
		c := newPWConn("b0")
		h := startPW(t, c, multi)
		var keys []string
		for i := range 8 {
			m := pwInsertBatch(i)
			keys = append(keys, pwKey(m))
			require.NoError(t, h.pw.ScheduleDataMessage(m))
		}
		require.NoError(t, h.pw.ScheduleBarrier(pwTaskBarrier(7)))
		pwRecv(t, c.gates["b0"].entered, "held batch entered")
		pwAssertNoFire(t, c)
		c.open()
		f := pwRecv(t, c.fired, "task handler")
		for _, k := range keys {
			assert.True(t, f.seen[k], k)
		}
		require.NoError(t, h.stop())
		assert.Empty(t, c.fired)
	})
}

func TestParallelWriterSecondCDCBarrierIsRejectedWhileFirstIsPending(t *testing.T) {
	pwModes(t, func(t *testing.T, multi bool) {
		per := pwUpdatesPerWorker()
		c := newPWConn(pwKey(per[0][0]))
		h := startPW(t, c, multi)
		pwHoldPrimer(t, h, c, per[0][0])
		require.NoError(t, h.pw.ScheduleBarrier(pwCDCBarrier("t1")))
		err := h.pw.ScheduleBarrier(pwCDCBarrier("t2"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "already being processed")
		c.open()
		assert.Equal(t, []byte("t1"), pwRecv(t, c.fired, "first token").msg.BarrierCdcResumeToken)
		require.NoError(t, h.pw.ScheduleBarrier(pwCDCBarrier("t3")))
		assert.Equal(t, []byte("t3"), pwRecv(t, c.fired, "third token").msg.BarrierCdcResumeToken)
		require.NoError(t, h.stop())
		assert.Empty(t, c.fired)
	})
}

func TestParallelWriterDuplicateTaskBarrierIsRejectedWhilePending(t *testing.T) {
	pwModes(t, func(t *testing.T, multi bool) {
		per := pwUpdatesPerWorker()
		c := newPWConn(pwKey(per[0][0]))
		h := startPW(t, c, multi)
		pwHoldPrimer(t, h, c, per[0][0])
		require.NoError(t, h.pw.ScheduleBarrier(pwTaskBarrier(5)))
		err := h.pw.ScheduleBarrier(pwTaskBarrier(5))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "already present")
		c.open()
		assert.Equal(t, uint(5), pwRecv(t, c.fired, "task handler").msg.BarrierTaskId)
		require.NoError(t, h.stop())
		assert.Empty(t, c.fired)
	})
}

func TestParallelWriterBarrierHandlerErrorFailsTheFlow(t *testing.T) {
	pwCases(t, func(t *testing.T, barrier iface.DataMessage, multi bool) {
		c := newPWConn()
		c.barrierErr = errPWInjected
		h := startPW(t, c, multi)
		require.NoError(t, h.pw.ScheduleBarrier(barrier))
		pwRecv(t, c.fired, "barrier handler")
		pwRecv(t, c.failed, "HandlerError")
		assert.ErrorIs(t, h.stop(), errPWInjected)
		handled := c.handledErrs()
		require.Len(t, handled, 1)
		assert.ErrorIs(t, handled[0], errPWInjected)
	})
}

func TestParallelWriterBlockBarrierReturnsOnlyAfterEveryWorkerFlushed(t *testing.T) {
	pwModes(t, func(t *testing.T, multi bool) {
		per := pwUpdatesPerWorker()
		primer := per[0][0]
		c := newPWConn(pwKey(primer))
		h := startPW(t, c, multi)
		pwHoldPrimer(t, h, c, primer)
		keys := pwScheduleAll(t, h, per, primer)
		res := make(chan error, 1)
		go func() { res <- h.pw.ScheduleBarrier(pwBlockBarrier()) }()
		select {
		case err := <-res:
			t.Fatalf("block barrier returned while a write was held: %v", err)
		case <-time.After(pwNoFireFor):
		}
		c.open()
		require.NoError(t, pwRecv(t, res, "block barrier return"))
		for _, k := range append(keys, pwKey(primer)) {
			assert.True(t, c.has(k), k)
		}
		require.NoError(t, h.stop())
	})
}

func TestParallelWriterBlockBarrierReturnsWriterClosedOnCancelledContext(t *testing.T) {
	per := pwUpdatesPerWorker()
	primer, second := per[0][0], per[0][1]
	c := newPWConn(pwKey(primer), pwKey(second))
	h := startPW(t, c, false)
	pwHoldPrimer(t, h, c, primer)
	require.NoError(t, h.pw.ScheduleDataMessage(second))
	res := make(chan error, 1)
	go func() { res <- h.pw.ScheduleBarrier(pwBlockBarrier()) }()
	pwWaitQueued(t, h, 0, 2)
	c.gates[pwKey(primer)].open()
	pwRecv(t, c.gates[pwKey(second)].entered, "barrier flush write entered")
	h.cancel()
	assert.ErrorIs(t, pwRecv(t, res, "block barrier return"), WriterClosedErr)
	c.open()
	assert.NotErrorIs(t, h.stop(), errPWJoinTimeout)
}

func TestParallelWriterTaskAndCDCBarriersInFlightTogether(t *testing.T) {
	pwModes(t, func(t *testing.T, multi bool) {
		per := pwUpdatesPerWorker()
		held := per[0][0]
		c := newPWConn(pwKey(held))
		h := startPW(t, c, multi)
		var batches []string
		for i := range 6 {
			m := pwInsertBatch(i)
			batches = append(batches, pwKey(m))
			require.NoError(t, h.pw.ScheduleDataMessage(m))
		}
		require.NoError(t, h.pw.ScheduleBarrier(pwTaskBarrier(3)))
		updates := pwScheduleAll(t, h, per, iface.DataMessage{})
		require.NoError(t, h.pw.ScheduleBarrier(pwCDCBarrier("tok")))

		task := pwRecv(t, c.fired, "task handler")
		assert.Equal(t, uint(iface.BarrierType_TaskComplete), task.msg.BarrierType)
		for _, k := range batches {
			assert.True(t, task.seen[k], k)
		}
		assert.False(t, task.seen[pwKey(held)])
		pwAssertNoFire(t, c)

		c.open()
		cdc := pwRecv(t, c.fired, "cdc handler")
		assert.Equal(t, uint(iface.BarrierType_CdcResumeTokenUpdate), cdc.msg.BarrierType)
		for _, k := range append(batches, updates...) {
			assert.True(t, cdc.seen[k], k)
		}
		require.NoError(t, h.stop())
		assert.Empty(t, c.fired)
	})
}
