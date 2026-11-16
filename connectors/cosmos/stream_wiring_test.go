package cosmos

import (
	"context"
	"errors"
	"testing"
	"time"

	mongoconn "github.com/adiom-data/dsync/connectors/mongo"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type fakeEvent struct {
	event     mongoconn.MongoUpdate
	decodeErr error
	remaining int
	token     bson.Raw
}

type fakeStream struct {
	events []fakeEvent
	pos    int
	err    error
}

func (f *fakeStream) Next(context.Context) bool {
	if f.pos >= len(f.events) {
		return false
	}
	f.pos++
	return true
}

func (f *fakeStream) current() fakeEvent { return f.events[f.pos-1] }

func (f *fakeStream) Decode(v any) error {
	e := f.current()
	if e.decodeErr != nil {
		return e.decodeErr
	}
	*(v.(*mongoconn.MongoUpdate)) = e.event
	return nil
}

func (f *fakeStream) RemainingBatchLength() int { return f.current().remaining }
func (f *fakeStream) ResumeToken() bson.Raw     { return f.current().token }
func (f *fakeStream) Err() error                { return f.err }

type flushed struct {
	updates []*adiomv1.Update
	token   bson.Raw
}

func recordFlushes(out *[]flushed) func([]*adiomv1.Update, bson.Raw) error {
	return func(u []*adiomv1.Update, tok bson.Raw) error {
		*out = append(*out, flushed{u, tok})
		return nil
	}
}

func goodEvent(remaining int, token string) fakeEvent {
	return fakeEvent{
		event: mongoconn.MongoUpdate{
			DocumentKey:  bson.D{{Key: "_id", Value: int32(1)}},
			FullDocument: bson.Raw(bsoncoreDoc(bson.D{{Key: "_id", Value: int32(1)}})),
		},
		remaining: remaining,
		token:     bson.Raw(token),
	}
}

func bsoncoreDoc(d bson.D) []byte {
	b, err := bson.Marshal(d)
	if err != nil {
		panic(err)
	}
	return b
}

var testLoc = iface.Location{Database: "db1", Collection: "col1"}

func TestStreamNamespaceEventsStopsOnFailureAfterBufferedEvent(t *testing.T) {
	for name, bad := range map[string]fakeEvent{
		"decode":  {decodeErr: errors.New("bad bson")},
		"convert": {event: mongoconn.MongoUpdate{FullDocument: bson.Raw{}}},
	} {
		t.Run(name, func(t *testing.T) {
			bad.remaining = 0
			stream := &fakeStream{events: []fakeEvent{goodEvent(1, "t1"), bad, goodEvent(0, "t3")}}
			var out []flushed

			err := streamNamespaceEvents(context.Background(), stream, testLoc, recordFlushes(&out))

			require.ErrorContains(t, err, "db1.col1")
			require.Empty(t, out)
			require.Equal(t, 2, stream.pos)
		})
	}
}

func TestStreamNamespaceEventsReturnsStreamError(t *testing.T) {
	boom := errors.New("stream broke")
	stream := &fakeStream{events: []fakeEvent{goodEvent(1, "t1")}, err: boom}
	var out []flushed

	err := streamNamespaceEvents(context.Background(), stream, testLoc, recordFlushes(&out))

	require.ErrorIs(t, err, boom)
	require.ErrorContains(t, err, "db1.col1")
	require.Empty(t, out)
}

func TestStreamNamespaceEventsFlushesEventWithoutFullDocumentAtBatchEnd(t *testing.T) {
	noDoc := fakeEvent{
		event:     mongoconn.MongoUpdate{DocumentKey: bson.D{{Key: "_id", Value: int32(2)}}},
		remaining: 0,
		token:     bson.Raw("t2"),
	}
	stream := &fakeStream{events: []fakeEvent{goodEvent(1, "t1"), noDoc}}
	var out []flushed

	err := streamNamespaceEvents(context.Background(), stream, testLoc, recordFlushes(&out))

	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Len(t, out[0].updates, 2)
	require.Equal(t, adiomv1.UpdateType_UPDATE_TYPE_PARTIAL_UPDATE, out[0].updates[1].Type)
	require.Equal(t, bson.Raw("t2"), out[0].token)
}

func TestDeliverEmulatedDeletesReturnsCheckErrorWithoutSending(t *testing.T) {
	checkErr := errors.New("check failed")
	sent := 0

	err := deliverEmulatedDeletes(
		func() (map[iface.Location][]*adiomv1.Update, error) { return nil, checkErr },
		func(iface.Location, []*adiomv1.Update) error { sent++; return nil },
	)

	require.ErrorIs(t, err, checkErr)
	require.Zero(t, sent)
}

func TestDeleteUpdatesFromIDsReportsMarshalErrorAndDrainsProducer(t *testing.T) {
	ch := make(chan idsWithLocation)
	stop := make(chan struct{})
	exited := make(chan struct{})
	t.Cleanup(func() {
		close(stop)
		<-exited
	})
	go func() {
		defer close(exited)
		defer close(ch)
		for _, ids := range [][]any{{int32(1), make(chan int), int32(3)}, {int32(4)}, {int32(5)}} {
			select {
			case ch <- idsWithLocation{loc: testLoc, ids: ids}:
			case <-stop:
				return
			}
		}
	}()

	updates, err := deleteUpdatesFromIDs(ch)

	require.ErrorContains(t, err, "db1.col1")
	require.Nil(t, updates)
	select {
	case <-exited:
	case <-time.After(5 * time.Second):
		t.Fatal("producer blocked")
	}
}
