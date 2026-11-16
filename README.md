**Welcome to Adiom's Dsync GitHub Repository!**

_Complete documentation can be found [here](https://docs.adiom.io)_

We solve data migration and replication between databases. We focus on challenges specific to large datasets and mission-critical applications. Our mission is to make database migration and replication easy for engineers and DevOps, reducing the need for custom solutions.

# Introduction to Dsync

Dsync is an open-source command line tool developed by [Adiom](https://adiom.io). Dsync is designed for seamless, fast, secure data migration and synchronization between databases and data stores, ensuring resiliency, ease of use, and production-grade performance. Technically speaking, Dsync is a **parallelized** in-memory **streaming engine** with **dynamic typing**. 

Dsync features first-class support for **NoSQL and SQL databases**, such as MongoDB, DocumentDB, Cosmos DB NoSQL, HBase, DynamoDB, PostgreSQL, SQL Server, Oracle, as well as **vector stores** such as Qdrant, Weaviate, S3 Vector store. 

Data copy and database migration jobs that previously took weeks, can now **be done in one afternoon** with a single command.

Common use cases for dsync:
* Live database migration
* Database replication
* Data mobility

Dsync is lightweight, flexible, and supports external connectors and transformers via gRPC. To run dsync, you just need to run the binary on your laptop, VM, or a docker container. Compute (CPU and RAM) is the only requirement, no storage is needed (except for logs)! 
> [!TIP]
> [**Enterprise edition** of Dsync](https://docs.adiom.io/enterprise/scalable-deployment) brings horizontal scalability, advanced observability, and transformations, while maintaining the same unparalleled ease of use.

![image](https://github.com/user-attachments/assets/5ad786fb-c180-4716-a443-e7bb0fef3619)

Given source and destination databases, dsync completes an initial sync transferring all data from the source database to the destination database. After the initial sync, it continuously monitors the source database for any changes and updates the destination database accordingly.

![Dsync progress reporting sample](/img/screenshot.png?width=20&raw=true)

## Features

* Highly parallelized initial sync
* Automatic transition to CDC with no user action needed
* Optimized writes
* Resumability both during initial sync and during CDC
* Reverse sync
* JSON and BSON support
* Transformers (including [CEL-based transformer](https://github.com/adiom-data/public/tree/main/dsync-transform) in Enterprise version)
* Observability
* Embedded verifiers
* Only compute needed - run it in VM, Docker, Kubernetes. No persistent storage required

Additionally, dsync supports load levels, rate limiting, namespace filtering and remapping, and query-based filtering for some connectors.

## Supported Connectors

### System

<table><thead><tr><th width="139.41015625">Type</th><th>Version</th><th>Status</th><th>Capabilities</th><th width="183.00390625">Notes</th></tr></thead><tbody><tr><td>/dev/null</td><td>-</td><td>Available</td><td>Source</td><td>Does exactly what you'd expect it to do</td></tr><tr><td>/dev/random</td><td>-</td><td>Available</td><td>Sink</td><td>Generates a stream of random operations</td></tr></tbody></table>

### NoSQL Databases

<table><thead><tr><th width="139.41015625">Type</th><th>Version</th><th>Status</th><th>Capabilities</th><th width="183.00390625">Notes</th></tr></thead><tbody><tr><td>MongoDB</td><td>4.2+</td><td>Available</td><td>Source, Sink</td><td>Supports Atlas dedicated, Atlas serverless and self-managed installations </td></tr><tr><td>Cosmos DB RU (MongoDB API)</td><td>4.2, 6.0+</td><td>Available</td><td>Source, Sink</td><td>Supports Azure Cosmos with Mongo API with Provisioned RUs (not serverless)</td></tr><tr><td>Azure Document DB (Cosmos DB vCore)</td><td>current</td><td>Available</td><td>Source, Sink</td><td>Both managed and Open Source versions are supported</td></tr><tr><td>Cosmos DB NoSQL</td><td>current</td><td>Available</td><td>Source, Sink</td><td></td></tr><tr><td>DynamoDB</td><td>current</td><td>Available</td><td>Source, Sink</td><td></td></tr><tr><td>HBase</td><td>1.x, 2.x</td><td><em>Private Preview</em></td><td>Source, Sink</td><td>Includes CDC support<br>See <a href="https://www.adiom.io/post/hbase-to-mongodb-migration">blog</a> for details</td></tr><tr><td>AWS DocumentDB</td><td>4.0, 5.0</td><td>Available</td><td>Source, Sink</td><td>Support via generic MongoDB connector</td></tr><tr><td>AstraDB</td><td>current</td><td><em>Private Preview</em></td><td>Source</td><td></td></tr><tr><td>Cassandra</td><td>4.0 or later</td><td><em>Private Preview</em></td><td>Source, Sink</td><td></td></tr></tbody></table>

### SQL Databases / RDBMS

<table><thead><tr><th width="139.41015625">Type</th><th>Version</th><th>Status</th><th>Capabilities</th><th width="183.00390625">Notes</th></tr></thead><tbody><tr><td>PostgreSQL</td><td>15+</td><td>Available</td><td>Source, Sink</td><td>Direct connectivity for 1:1 migrations with transformations</td></tr><tr><td>SQL Server / PostgreSQL / Oracle / DB2</td><td>-</td><td><em>Public Preview</em></td><td>Source</td><td>Custom query-based via our "SQL batch" connector<br>See <a href="https://www.adiom.io/post/migrate-rdbms-to-mongodb">blog</a> and <a href="https://docs.adiom.io/reference/connectors/sql-batch">docs</a> for details.</td></tr></tbody></table>

### Vector Databases

<table><thead><tr><th width="139.41015625">Type</th><th>Version</th><th>Status</th><th>Capabilities</th><th width="183.00390625">Notes</th></tr></thead><tbody><tr><td>Weaviate</td><td><em>latest</em></td><td>Public Preview</td><td>Sink</td><td></td></tr><tr><td>Qdrant</td><td><em>latest</em></td><td><em>In Development</em></td><td>Sink</td><td></td></tr><tr><td>S3 Vector Index</td><td>-</td><td><em>Public Preview</em></td><td>Sink</td><td></td></tr></tbody></table>

### Other

<table><thead><tr><th width="139.41015625">Type</th><th>Version</th><th>Status</th><th>Capabilities</th><th width="183.00390625">Notes</th></tr></thead><tbody><tr><td>S3 Storage</td><td><em>-</em></td><td>Available</td><td>Source, Sink</td><td>Export into and from S3 in JSON format. CDC isn't supported: use "--mode InitialSync"</td></tr><tr><td>File</td><td>-</td><td>Available</td><td>Source, Sink</td><td>Only CSV for now</td></tr></tbody></table>


## PostgreSQL document mode

With `--document-mode`, the PostgreSQL destination stores each source document as one row instead of mapping fields to columns. The destination runs no DDL: create the table beforehand.

Table layout:
* `id text`, holding the document `_id` in an encoded text form.
* `doc jsonb`, holding the whole document as canonical Extended JSON.
* The primary key is `id`, or `id` plus other columns of the table.
* Optional extra columns named after top-level document fields. They are filled from the field of the same name, or set to NULL when the field is absent or null.
* Optional timestamp column for `--updated-at-column`.

Flags:
* `--document-mode` enables the mode.
* `--updated-at-column <column>` sets a `timestamp with time zone` or `timestamp without time zone` column when a row is written or changed. Unchanged rows are not rewritten. Needs `--document-mode`.
* `--updated-at-field <field>` takes that value from a top-level date field of the document instead of the transfer time (UTC). If the field is absent or null, the transfer time (UTC) is used; a non-date value is rejected. Needs `--updated-at-column`.

Limits:
* Namespaces must be two-part `schema.table` names.
* Writable extra columns that are not part of the primary key must be one of: text, character varying, character, smallint, integer, bigint, real, double precision, numeric, boolean, timestamp with or without time zone, date, bytea, uuid. Domains, arrays, json and jsonb are rejected. Primary-key columns are exempt from this list. Generated columns and `GENERATED ALWAYS` identity columns are not written. A `GENERATED BY DEFAULT` identity column is written like any other extra column, so a document lacking that field or holding null in it fails the write.
* A deferrable primary key is rejected.
* Documents containing Extended JSON wrapper names as keys (such as `$oid` or `$numberInt`) are rejected.
* Updates are applied as whole documents. Non-empty partial updates are rejected; empty ones are skipped. In this repository only external connectors can produce non-empty partial updates. The MongoDB and Cosmos DB sources emit empty ones when a change event has no full document.
* With a composite primary key, every extra key column must also arrive in the update's id as a part named after the column (for a MongoDB source: the shard key fields, with `--full-document-key`). Otherwise deletes are rejected and the flow stops. Inserts and updates take the value from the document.
* By default the PostgreSQL connector runs `SET LOCAL session_replication_role = 'replica'` inside each destination write transaction (`--enable-replica-mode`, default true); the source is unaffected. This needs superuser or a SET grant on that parameter, and disables triggers and foreign key checks for its writes. A role without that privilege fails at the first write, not at connect; `--enable-replica-mode=false` only matters for the destination. With triggers disabled, use `--updated-at-column` to get a modification time.
* If a key column value changes at the source, the old row stays and a new one is written.
* The source must send an id part named `_id` (or a single unnamed id part). Sources whose id parts are named after other columns, such as the SQL batch connector, cannot feed this mode.

### Replay after restart

After a restart the transfer resumes from the last checkpoint, so writes made after it are applied again.
* Column mode rewrites every writable non-key column on each applied write; identity and generated columns keep their value. A column omitted from a message gets its default again, or NULL when it is nullable and has no default. Sequence or time defaults produce a new value on each replay.
* Document mode without `--updated-at-field` leaves the `--updated-at-column` value at the replay time whenever the replayed window holds more than one version of a document, the newest included. With `--updated-at-field`, the value comes from the document when that field holds a date; a missing or null field falls back to the write time, and a non-date value fails the write.
* A value moved from one row to another under a secondary UNIQUE constraint inside the replayed window can fail the replay with SQLSTATE 23505.

## PostgreSQL state store

An alternative to the MongoDB `--metadata` store. It keeps dsync flow state in one PostgreSQL table, `dsync_state.objects`. Without any metadata setting dsync uses a temporary store, and flow state does not survive a restart.
* Select it by setting the environment variable `DSYNC_METADATA_POSTGRES` to a PostgreSQL connection string. The value is read only from the environment: there is no command-line flag, and a `--config` file key is ignored.
* Setting it together with `--metadata` is an error.
* The connection string is never printed. Database errors carry a fixed message, plus the SQLSTATE when the server returned one, or otherwise a failure class (could not connect, network timeout, context cancelled or deadline exceeded).
* dsync runs no DDL. Setup only checks that the table can be read, and otherwise fails with a pointer to this section. Missing INSERT or UPDATE privileges surface at the first write, DELETE at the first delete, with SQLSTATE 42501. A failed delete of the flow state on `--cleanup` is only logged: the run exits 0, the row stays, and the next run with the same options resumes that flow.
* It can live on the same server as the PostgreSQL destination; it uses its own connection string.
* Values are stored as BSON in `data`. `updated_at` is the time of the last write.
* After setup, these errors are retried: SQLSTATE class 08 (connection exceptions), 40001, 40P01, 55P03, 57P01, 57P02, 57P03, 57P05, 25P03, 53300 and 53200; network errors and unexpected EOF; and attempt timeouts that surface as a context deadline. Other errors, including 53100, 53400 and 57014, fail immediately. Retries run under `--meta-retries` and `--meta-retry-max-interval`, with a 30 s timeout per attempt. The setup check itself is not retried.
* The store uses pgx's default prepared-statement cache, so a pooler in transaction mode (PgBouncer and similar) is not supported; connect directly or through session pooling.

Setup script, run once as a role with CREATE on the database (`dsync_state_writer` is a placeholder for the role in the connection string):
```sql
CREATE SCHEMA dsync_state;
CREATE TABLE dsync_state.objects (
    store text NOT NULL,
    id text NOT NULL,
    data bytea NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (store, id)
);
GRANT USAGE ON SCHEMA dsync_state TO dsync_state_writer;
GRANT SELECT, INSERT, UPDATE, DELETE ON dsync_state.objects TO dsync_state_writer;
```

## Verify

`dsync verify [verify flags] <left> [left connector flags] <right> [right connector flags]` compares two connectors document by document. It reads both sides, keeps comparing while their change streams run, and reports mismatches. Documents are matched by `_id` only. Verify flags (`--namespaces`, `--once`, `--latency`, `--simple` and so on) go first: flag parsing stops at the first connector, and flags after it belong to that connector.

Exit status: 0 when the last verdict is clean, 1 on any mismatch, source error or usage error. A mismatch error reads `verify found mismatches: N mismatches in M checked`; it carries counts only, and the ids are in the log lines above it.

`--once`:
* Waits for both initial syncs, then `--latency` plus one second, takes one report, stops both sources, and exits with that verdict.
* Cannot be combined with `--simple`: that is a usage error raised before any connector is touched.
* Changes that arrived within the last `--latency` before the report are not judged and count as clean, even when the initial read had found that document different. A change that reaches one side after the report is not seen at all, so run `--once` when the sides are expected to have converged.

`--latency`:
* Stream changes are judged only after they are `--latency` old, so it must be positive in memory mode (a usage error otherwise). With 0 a stream change would clear a mismatch and never be compared. `--simple` accepts 0.

Signals:
* SIGINT and SIGTERM end the run with a verdict. Memory mode uses the last report; `--simple` uses all reports so far (any diff counts). An interrupt before the first report is an error.
* A second Ctrl-C kills the process and skips cleanup.

PostgreSQL as a verify side:
* `--document-mode`, `--manual`, `--replication-slot` and `--publication-name` are PostgreSQL connector flags: put them after `postgresql://...` on that side, for example `dsync verify --namespaces db.t postgresql://... --document-mode --replication-slot verify_left_slot mongodb://...`. `--partition` is a verify flag and goes first.
* A document-mode table is read back as documents (see PostgreSQL document mode). Pass `--document-mode` on that side to compare document tables: column mode does not restore the original BSON types.
* Unless `--manual` is set, verify creates a replication slot and a publication for its change stream, and drops both on exit, whatever the verdict. Cleanup runs after the verifier returned; a slot still held by the closing replication connection is retried for about 5 seconds. A failed drop is logged at error level and does not change the verdict or the exit status. Nothing is dropped with `--manual`, and remote (gRPC) connectors get no cleanup.
* Use `--replication-slot` and `--publication-name` values that differ from any running sync and from other verify runs. With the default names next to a running sync, verify recreates the publication the sync uses and drops it on exit. When both verify sides are PostgreSQL on one server they need distinct values too: with the same defaults the right side's create drops the left side's slot. Parallel `--partition` runs against one server each need their own: a run's create drops an existing slot and publication of the same name, including another run's.
* If cleanup failed or was skipped (for example after a second Ctrl-C), drop them by hand; a leftover slot retains WAL on the server:
```sql
SELECT pg_drop_replication_slot('<slot>');
DROP PUBLICATION <name>;
```

Requirements and limits:
* Set `REPLICA IDENTITY FULL` on document-mode tables. Without it a delete carries only the key columns, and a numeric `_id` in a delete arrives as int64. With `--partition` that id hashes to another partition, and the owning run reports the document as a permanent mismatch.
* TRUNCATE on a published table stops the stream with an error. Restart verify; it reads the table again.
* A collection whose `_id` repeats under different shard key values is not comparable, because documents are matched by `_id` only.
* Sources whose multi-part ids have no part named `_id` are not supported.

# Quickstart
Follow these simple instructions to get dsync up and running and perform a migration from our Cosmos DB demo instance to your MongoDB destination.

## 1. Get Dsync

You can build dsync from the source to get all the latest changes or a specific release using tags: 
```
git clone https://github.com/adiom-data/dsync.git
cd dsync
go build
```

Alternatively, you can use docker to run the latest release:
```
docker run --network host --rm markadiom/dsync
```

You can also download the latest release as a binary from the [GitHub Releases](https://github.com/adiom-data/dsync/releases/latest) page.

> [!TIP]
> You can use Homebrew to easily install Dsync on your Mac:
> 
> `brew install adiom-data/homebrew-tap/dsync`

## 2. (Optional) Prepare the destination MongoDB instance

If you already have the desired destination MongoDB instance up and running, you can skip this step. Otherwise, use the following steps to launch MongoDB on your local machine:

1) Install [MongoDB](https://www.mongodb.com/docs/manual/administration/install-community/) 
2) Start a local MongoDB instance
```
mkdir ~/temp
cd ~/temp
mkdir data_d
mongod --dbpath data_d --logpath mongod_d.log --fork --port 27017
```

## 3. Start dsync
```
# You can use our publicly accessible (read-only) Cosmos DB instance
export COSMOS_DEMO=$(echo bW9uZ29kYjovL2Nvc21vc2RiLWRlbW8taW5zdGFuY2U6SkhiRWpRb2JkWm03YWJEcFp2UzZrWHpBMDRXNTBJd2V4QmlQYnVJWFQ2TElmNkhsV2V4YWphQzhkd042REJ2YVh6ajBnclFrdkwzY0FDRGJONjdxZWc9PUBjb3Ntb3NkYi1kZW1vLWluc3RhbmNlLm1vbmdvLmNvc21vcy5henVyZS5jb206MTAyNTUvP3NzbD10cnVlJnJlcGxpY2FTZXQ9Z2xvYmFsZGImcmV0cnl3cml0ZXM9ZmFsc2UmbWF4SWRsZVRpbWVNUz0xMjAwMDAmYXBwTmFtZT1AY29zbW9zZGItZGVtby1pbnN0YW5jZUA= | base64 --decode)

# Feel free to use your own MongoDB connection string
export MDB_DEST='mongodb://localhost:27017' 

./dsync --progress --logfile dsync.log $COSMOS_DEMO $MDB_DEST
```
Now Dsync should be running! Feel free to interrupt the sync process (via Ctrl+C) it once the initial sync is done. The demo database has a few million records and the resources are shared - allow 5-10 minutes for the process to complete.

## 4. Check the data
```
mongosh $MDB_DEST
```
Congratulations! You should be able to access the 'odc' database and see the collections in it that were migrated from the Cosmos DB.

# Development Tips

Use `docker-compose up` to start up the docker containers which run multiple mongo instances:
* mongo1 - used as a metadata store and as a potential source/sink
* mongo2 - used as a potential source/sink
* mongotest - used by the mongo tests

If you are developing on a mac, you likely need to edit your /etc/hosts file so that the docker hostnames work. Add this line:

```
127.0.0.1       mongo1 mongo2 mongotest
```

You need to export COSMOS_TEST to be able to run the cosmos test. For DynamoDB you need to have localstack running.

```
make clean-testdb # cleans and populates mongotest for the mongo connector test
make test # just tests without external dependencies
make test-all # if you have everything set up properly, you can run this
# test specific connectors
make test-mongo
make test-cosmos
make test-dynamodb
```

## Mocks

First time install mockery: `brew install mockery`

After just run `make mocks`

## General testing with the docker instances

```
make clean-dbs
make verify # should succeed
make boostrap1
make verify # verify should fail
make boostrap2
make verify # verify should succeed
make clean-dbs
make bootstrap1
make sync12 # syncs mongo1 to mongo2, have to ctrl-C after a few seconds to stop
make verify # verify should suceed
```


# Questions and reporting issues

Please report any bugs here in the GitHub project.

For technical questions and discussions, please join our [Discord server](https://discord.gg/r4xzVfMQeU).

For commercial inquires, please contact info@adiom.io.
