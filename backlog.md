# Backlog

Open work for the MongoDB / Cosmos DB → PostgreSQL path: decisions put off, review findings left unfixed, checks not yet run. Numbers in brackets are the pull requests of this repository where the item was found. Items are grouped by area; inside a group the order is by priority.

## Decisions waiting for data

### Emulated deletes on large collections [#27]

A delete pass reads every key of every table, so its cost grows with the row count. It is safe only after writes to the source have stopped, which puts two full passes inside the write freeze.

* Measure first, in a rehearsal on the largest collection: change-stream catch-up, destination drain, witness scan and source lookup throughput, memory per pass, `verify --once`, building UNIQUE constraints.
* Candidate change, if the freeze does not fit: a pass drops its deletes for keys the stream has sent since that pass started. This would let the witness run while the source takes writes. To prove before building it:
    * the destination applies a delete from a pass and a stream write of the same key in the order they were sent (both must reach the same writer worker, so their id bytes must be equal);
    * the source lookup of a pass does not return stale data.
* Rejected for now as too large: a durable ledger of deleted keys kept on the source side.

### Dotted shard key fields [#22, #27]

The witness and the Cosmos DB stream read a field `a.b` as a nested path; the document-mode sink looks up a top-level field named `a.b`. A document that holds the value only as nested `a` → `b` stops the flow.

* Take an inventory of shard keys of the collections to migrate.
* If none is dotted: reject a dotted key column when the table shape is loaded, with a clear error.
* If some are: read key columns as nested paths in both the insert path and the update key check, with no literal-name fallback, and add tests that copy, update, delete and the witness agree (null, missing and ambiguous fields included). The helper document the Cosmos DB connector writes builds dotted fields literally and needs the same treatment.

### Server-supplied names in error text [#28]

Server errors print the SQLSTATE plus schema, table, column and constraint names. A reviewer asked to drop the names too: a schema named after a login role shows that role, and server-side code can fill these fields with any text. Kept for diagnosis; revisit if such schemas appear.

### Document mode contract [#22]

To confirm with the owners of the services that will read the tables:

* Documents are stored as canonical Extended JSON in `jsonb` (`{"$numberInt":"2"}`); the witness and verify need the BSON types. Readers may want the relaxed form.
* int32, int64 and whole doubles of the same value share one id; a fractional double and a decimal128 of the same value are different ids.
* An extra column whose field is absent or null is written as NULL, which bypasses a column DEFAULT and fails on `NOT NULL DEFAULT`.
* A changed key-column value at the source leaves the old row in place.
* The table shape is read once; a schema change during a run needs a restart.
* Without `--updated-at-field` the updated-at column holds the transfer time, not the time of the change.

## Checks not yet run

Run locally in containers so far: connection error texts, state store with wrong credentials and missing privileges, copy + live changes + `kill -9` + resume, `verify --once` on equal and unequal data, slot and publication cleanup after an interrupted verify.

* Against the real source:
    * shard key inventory and collection sizes;
    * whether `documentKey` carries the shard key on change-stream events [#21];
    * whether a collection with no recent changes yields a resume token when the source key is read-only: one namespace without a token fails the whole plan [#21];
    * `$sample` cost and behaviour, string order under a non-default collation, cross-type sort order [#21];
    * read consistency of the source lookup in a delete pass [#27];
    * request-unit cost and throttling of a delete pass.
* Against the real destination service:
    * `--enable-replica-mode` with an unprivileged role, end to end [#22];
    * connections through the service's pooler: destination, witness, state store, replication [#25, #28];
    * `default_query_exec_mode=exec` in the connection string;
    * an error raised by a TLS refusal, a server restart, a failover.
* Emulated deletes end to end: single `_id` key and composite key, duplicate `_id` under different shard key values, an interrupted and repeated pass, then `deletes=0 deferred=0` [#27].
* Full cutover rehearsal with timings.
* A state store role with SELECT only on a resumed idle flow: the run did not fail within a minute in a local check. Find out whether no write was attempted or a failure was swallowed [#25].
* Tests behind the `external` build tag compile but were never run: `replication_cleanup_external_test.go`, `keylister_external_test.go` [#26, #27].
* Streams with only empty transactions for longer than the standby status interval [#20].
* The server `ErrorResponse` branch of the replication receive loop, the log line on batch close and the post-connect returns of the stream are covered by unit tests or by structure only [#20, #28].
* A namespace collision through the CLI; the transform refusal through `ConfigureConnectors`; that `verify` and `serve` ignore the witness variable [#27].
* No unit test builds three or more sampled ranges; a sampler error and "too few samples" take the same fallback and tests do not tell them apart [#21].

## PostgreSQL connector

### Errors and logs

* `LSN tracker: Failed to open change stream: context canceled` is logged at ERROR when a resumed flow starts. Cosmetic; lower the level or skip cancellation.
* No failure class for a TLS refusal by the server, EOF during startup or cancellation: the text is the bare `could not connect to postgres` [#28].
* `Unwrap` of a hidden error returns the original pgx error; code that prints inner errors of the chain would print the details. Nothing here does [#28].
* pgx keeps the password inside `ConnectError.Config`; formatting that error with `%+v` would print it. Nothing here does [#20].
* A mistyped URL (missing `:` or `@`) can place secret text in the host field [#20].
* The state store has its own fixed-text sanitizer, separate from the connector's; the SQL batch connector has none [#28].
* Caller context around a server error is printed as is; it is safe for the wrap sites that exist today. A new wrapper that puts sensitive text around a server error would print it [#28].
* `HideConnDetails` also hides server errors now; the name no longer says so [#28].
* Hiding a server error walks the error chain twice [#28].
* The unit tests of connection errors assume that port 1 on localhost refuses connections [#28].
* The `password=` guard covers global flags and the PostgreSQL connector's flags only, and refuses a harmless value containing `password=` [#22].
* On a failed batch the core writer logs the first message, id bytes included [#22].
* The publication name and namespaces are not validated at the flag level; they are printed in errors [#20].
* A fatal server error arriving while a later result is read names that later statement; an error raised only at commit names no position [#22].

### Retries

* `57014` (query cancelled) is not retried [#20].
* `08P01` and `25P03` are retried; a DNS "not found" is retried like any network error; network errors and EOF are retried even when pgx marks the call as not safe to retry. Writes are idempotent [#20].
* The `55006` exception covers any `55006` from the stream, not only a busy slot. Separately, the retry check does not treat that error as transient although its code says unavailable: decide what a busy slot should do [#20, #28].

### Document mode

* Names with dots or quotes, inheritance child tables and `USING INDEX` primary keys are not supported or not checked [#22].
* `$regex`/`$options` subdocuments are stored as plain documents; checked with the Go driver only [#22].
* The decode half of the id codec has no production caller [#22].
* The first table-shape loads are serialised by one mutex [#22].
* The SQL batch connector cannot feed document mode [#22].
* With PostgreSQL as a streaming source the moved columns come from the catalog as it is now, not as it was when the change was logged: a column in move mode added while the stream lags fails the replay until it gets a `copy` comment.
* Reading a document-mode table now loads the same table shape as writing, so a table the write path refuses (a deferrable primary key, a bad `dsync:` comment) is refused as a source too.
* A moved `text` column under a nondeterministic collation skips an update that changes only what the collation ignores (letter case, for example), because the upsert compares columns with the column's collation.
* A value in the replication stream that the driver cannot decode (a `jsonb` number outside the float64 range written by something other than dsync) fails with the driver's error text, which can quote the value.
* Move mode takes top-level fields only; a column cannot name a nested path.
* Move mode has no `numeric` pair: PostgreSQL normalises the value, so the read-back document would differ. Such columns stay in copy mode.
* Move mode stores `jsonb` columns as canonical Extended JSON; a plain JSON form would not survive `verify`.
* A NaN payload or other value that PostgreSQL normalises in a moved column is not checked for; read-back through logical replication with moved columns has unit coverage only, with values decoded by the text codecs.
* Without `--updated-at-field` the updated-at column ends at the replay time when the replayed window holds more than one version of a document [#24].
* Unique and exclusion indexes besides the primary key, on the table or its partitions, are refused before the first write to that table, not at flow start; the check is a second catalog query per table under the plan lock. A unique index whose key columns, not INCLUDE columns, contain every primary key column is refused too, though harmless. Foreign keys and CHECK constraints are not looked at; an index added while dsync runs is not seen until a restart.

### Column mode and PostgreSQL as a source

* Column mode does not refuse secondary unique indexes: a value moved between rows inside the replayed window fails the replay with SQLSTATE 23505 [#24].
* Omitted columns with a default are reset on every conflict, so sequence or time defaults change on every write and replay [#24].
* `TRUNCATE` is ignored; `streaming 'true'` is requested but stream commit and abort messages are not handled; composite primary keys with `timestamptz` are gob-encoded [#20].
* A source role with SELECT only on a table gets `no primary key found for namespace …`: the key lookup does not see constraints of tables the role has only SELECT on. Seen in a local check; the message should name the privilege.
* `connectors/postgres/conn.go` is above 800 lines [#20].

## Verify

* The PostgreSQL role must own the tables it verifies (it creates a publication); otherwise verify stops with SQLSTATE 42501. Not in the README yet [#26].
* Matching is on `_id` only: a collection holding one `_id` under several shard key values cannot be compared [#26].
* `--once` counts ids changed inside the last `--latency` as clean [#26].
* Structured ids are compared by a 64-bit hash [#26].
* With the default slot and publication names next to a running sync, verify recreates the sync's publication and drops it on exit. A create that fails on a name collision still drops that name on exit. A forced create drops a same-named slot of another run [#26].
* With `--partition` and no `REPLICA IDENTITY FULL`, a delete of an int32 id is routed to another partition's run; a numeric id arrives as int64 [#26].
* TRUNCATE stops the stream [#26].
* Column mode does not restore the original BSON types [#26].
* Sources whose multi-part ids have no part named `_id` are not supported [#26].
* A leftover slot retains WAL; the README has the manual drop commands [#26].

## State store

* Setup checks SELECT only; missing write privileges show at the first write [#25].
* The setup check is not retried [#25].
* An attempt that times out on the client may still commit on the server [#25].
* A failed state delete on `--cleanup` is only logged and the run exits 0 [#25].
* An unreachable host without `connect_timeout` reports a deadline, not "could not connect" [#25].
* Calls after `Teardown` fail with the generic request error [#25].
* No automated test runs the SQL against a real PostgreSQL [#25].

## Cosmos DB source and emulated deletes

* The connector inserts and deletes a helper document in the source collection to get a resume token. With a read-only source key that write does not happen and the token must come from the collection's own changes [#21].
* ObjectId ranges with no documents become tasks that still report the target document count, so progress estimates can be inflated [#21].
* The id sample is capped at 10000, so a collection gets at most about 10000 ranges [#21].
* Collections with mixed `_id` types are copied as one task [#21].
* The planner's debug log prints the smallest and largest `_id` [#21].
* With `--full-document-key`, shard keys are fetched one namespace at a time at plan time [#21].
* `StreamUpdates` and `GeneratePlan` are longer than 50 lines [#21].
* Resuming after a pass error needs a persistent state store and the same flow; otherwise a restart begins a fresh plan [#27].
* Check against the current delete pass whether these two notes still hold: a cursor error in the check drops the batch without failing the stream; the reply of its aggregate is read with an unchecked type assertion [#21].
* Any pass error, transient ones included, ends the stream; dsync has to be restarted [#27].
* The MongoDB witness does not apply `--namespace` and finds nothing, without an error, when names differ [#27].
* The same numeric `_id` stored as different BSON number types in source and witness aborts every pass [#27].
* A shard key field holding a subdocument is deferred on every pass; while an `_id` is deferred, a real delete of one of its keys is held back [#27].
* A document without a shard key field and one with an explicit null are the same key [#27].
* A witness table must hold rows of exactly one source namespace; any other row is deleted by the pass [#27].
* Emulated deletes are sent without a resume token [#27].
* The witness connection string has no command-line flag [#27].

## Core

* `UpdateCDCResumeToken` holds the coordinator flow lock across the state-store write: a slow store delays error reporting and status reads [#24].
* Read-plan publication and creation and `NotifyDone` run without the flow lock [#24].
* Routing by `_id` alone puts all documents sharing an `_id` on one writer worker [#24].
* The token map is gob-encoded under the stream lock on every batch; cost grows with the namespace count [#24].
* The first error on a stream is sticky, token-map and cursor encode failures included [#24].
* A writer worker that exits on context cancellation returns nil; the cancellation is reported on the producer side [#24].
* Emulated deletes are counted into the LSN only after their send succeeds [#24].

## Housekeeping

* Generated usage text is stale (`postgres.proto` for `--manual` and document mode, `--cosmos-deletes-cdc`): the generators were not available [#26, #27].
* `go vet ./...` and `gofmt -l` report problems in files none of these pull requests touch [#20, #21].
* This fork has no registered Actions: no checks run on pull requests.
