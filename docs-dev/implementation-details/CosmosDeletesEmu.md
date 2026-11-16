# Cosmos DB deletes emulation

As a work around the Cosmos DB changestream [limitation](https://learn.microsoft.com/en-us/azure/cosmos-db/mongodb/change-streams?tabs=javascript#current-limitations), dsync implements a specially-designed algorithm that detects deletes on the source and emits them on a periodic basis.

## Algorithm

- We use an external "Witness" index to track which documents we've already seen coming from the source.
    - The key assumption is that the set of keys in the Witness index is always a superset of the keys in the Source index (W = S + whatever was deleted)
    - For simplicity, we can use the destination's _id index directly as a Witness as it already has exactly what we need maintained with transactional semantics.
 - At any point in time, any document that is present in the Witness index but absent from the source is considered deleted on the source.

1. Take the namespaces the stream is running on.
2. For every namespace, walk all Witness keys once, in `_id` order, handed out in pages of 1000. Counts are not compared: a delete plus an insert keeps them equal.
3. Check each page against the Source with one `find` on `{_id: {$in: page}}` (projection `_id`) and take the set difference on the client. Ids are compared by value for numeric types (int32, int64, integral double) and by BSON type plus bytes otherwise. Keys absent from the result become delete messages, sent into the data channel with the Witness key's exact bytes.
4. Any error aborts the whole pass without emitting deletes, so a throttled or timed-out read is never taken for a missing document. A source `_id` that matches no key of the page (the server matched across types the client keeps apart, e.g. Decimal128 5 and int 5) aborts the pass as well, instead of deleting a live document.
5. After the deletes of a pass are sent, one line logs the number of namespaces walked, keys scanned and deletes emitted.

The Witness is an interface (`Witness` in `connectors/cosmos/witness.go`): scan a namespace's keys in key order, emitting pages. Paging is the implementation's concern. The MongoDB witness runs one sorted `find` per namespace and is the only implementation so far.

## Pros: 
- For users, there's no need to change the application code to use soft deletes.
- The algorithm works reliably and ensures eventual consistency.
- The behavior can be turned on/off anytime in the process without restarting the sync from scratch.

## Cons:
- Index scans can be expensive hence the check period may need to be adjusted to balance the lag and the responsiveness.