package cosmos

import (
	"context"
	"errors"
	"testing"

	"github.com/adiom-data/dsync/protocol/iface"
	"go.mongodb.org/mongo-driver/v2/bson"
)

var testLoc = iface.Location{Database: "labelprinter", Collection: "drinkit_labels"}

func uuidID(b byte) bson.Binary {
	return bson.Binary{Subtype: 4, Data: []byte{b, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}}
}

// lookups answers each call with the next canned set of present ids and records what it was asked.
type lookups struct {
	answers [][]interface{}
	asked   [][]interface{}
	err     error
}

func (l *lookups) find(_ context.Context, _ iface.Location, ids []interface{}) ([]interface{}, error) {
	l.asked = append(l.asked, ids)
	if l.err != nil {
		return nil, l.err
	}
	answer := l.answers[0]
	l.answers = l.answers[1:]
	return answer, nil
}

func noWait(context.Context) error { return nil }

func TestMissingInSourceKeepsAnIdTheFirstLookupDropped(t *testing.T) {
	a, b, c := uuidID(1), uuidID(2), uuidID(3)
	// Cosmos answered the first query without b although the document is there, as its aggregate did.
	l := &lookups{answers: [][]interface{}{{a, c}, {b}}}
	missing, err := missingInSource(context.Background(), l.find, testLoc, []interface{}{a, b, c}, noWait)
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != 0 {
		t.Fatalf("an id present on the recheck must not be deleted, got %v", missing)
	}
}

func TestMissingInSourceReturnsOnlyIdsAbsentFromBothLookups(t *testing.T) {
	a, b, c := uuidID(1), uuidID(2), uuidID(3)
	l := &lookups{answers: [][]interface{}{{a}, {}}}
	missing, err := missingInSource(context.Background(), l.find, testLoc, []interface{}{a, b, c}, noWait)
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != 2 || canonicalID(missing[0]) != canonicalID(b) || canonicalID(missing[1]) != canonicalID(c) {
		t.Fatalf("want b and c, got %v", missing)
	}
}

func TestMissingInSourceRechecksOnlyTheCandidates(t *testing.T) {
	a, b := uuidID(1), uuidID(2)
	l := &lookups{answers: [][]interface{}{{a}, {}}}
	if _, err := missingInSource(context.Background(), l.find, testLoc, []interface{}{a, b}, noWait); err != nil {
		t.Fatal(err)
	}
	if len(l.asked) != 2 || len(l.asked[1]) != 1 || canonicalID(l.asked[1][0]) != canonicalID(b) {
		t.Fatalf("the recheck must ask for the candidates only, asked %v", l.asked)
	}
}

func TestMissingInSourceSkipsTheRecheckWhenNothingIsMissing(t *testing.T) {
	a := uuidID(1)
	l := &lookups{answers: [][]interface{}{{a}}}
	if _, err := missingInSource(context.Background(), l.find, testLoc, []interface{}{a}, noWait); err != nil {
		t.Fatal(err)
	}
	if len(l.asked) != 1 {
		t.Fatalf("no candidates, no recheck; asked %d times", len(l.asked))
	}
}

func TestMissingInSourceMatchesIdsByBsonValueNotByIdentity(t *testing.T) {
	ids := []interface{}{uuidID(7), bson.NewObjectID(), "order-1", int32(42)}
	copies := make([]interface{}, len(ids))
	for i, id := range ids {
		// the source decodes its own values: equal by content, not the same objects
		raw, err := bson.Marshal(bson.M{"_id": id})
		if err != nil {
			t.Fatal(err)
		}
		var doc bson.M
		if err := bson.Unmarshal(raw, &doc); err != nil {
			t.Fatal(err)
		}
		copies[i] = doc["_id"]
	}
	l := &lookups{answers: [][]interface{}{copies}}
	missing, err := missingInSource(context.Background(), l.find, testLoc, ids, noWait)
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != 0 {
		t.Fatalf("equal ids decoded apart must match, got missing %v", missing)
	}
}

func TestMissingInSourceFailsClosedWhenTheLookupFails(t *testing.T) {
	l := &lookups{err: errors.New("request rate is large")}
	missing, err := missingInSource(context.Background(), l.find, testLoc, []interface{}{uuidID(1)}, noWait)
	if err == nil || missing != nil {
		t.Fatalf("a failed lookup must give an error and no deletes, got %v, %v", missing, err)
	}
}

func TestMissingInSourceFailsClosedWhenTheWaitIsCancelled(t *testing.T) {
	l := &lookups{answers: [][]interface{}{{}}}
	cancelled := func(context.Context) error { return context.Canceled }
	missing, err := missingInSource(context.Background(), l.find, testLoc, []interface{}{uuidID(1)}, cancelled)
	if err == nil || missing != nil {
		t.Fatalf("an interrupted recheck must give no deletes, got %v, %v", missing, err)
	}
}
