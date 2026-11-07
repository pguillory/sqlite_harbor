# SqliteHarbor

Many small SQLite databases across a BEAM cluster, backed by S3-compatible object
storage. Each active database has one owner process and one local SQLite connection.
Inactive databases cost one object. The API is create, delete, list, execute, export,
and import. There is no HTTP server or application-specific model.

**Early software.** The automated suite exercises real SQLite, faithful object-store
failures, lifecycle recovery and real BEAM peers. Real R2 conditional-write verification
is pending access keys. Run the [storage probe](#verify-your-bucket) before production
adoption. Production-host throughput and shutdown sizing are also unmeasured.

## Install

```elixir
{:sqlite_harbor, git: "https://github.com/pguillory/sqlite_harbor.git", branch: "main"}
```

This library pins a minimal [exqlite fork](https://github.com/pguillory/exqlite/tree/single-statement)
to an exact commit. It adds `prepare_single/2`, rejecting extra statements rather than
silently ignoring their SQL, exposes SQLite's per-connection length limit, and builds
the NIF from source. Its bounded `step/3` checks a whole row before copying any cells
to BEAM memory. A C compiler is required.
The normal `prepare/2` API is unchanged. If your host already uses exqlite (including
through Ecto), add the same fork/ref from this repository's mix.exs to your own deps
with `override: true`. Every exqlite consumer then uses that fork.
Git dependencies prevent Hex publication until this change is upstream. No Hex release
or upstream PR has been made.

## Start and query

```elixir
children = [
  {SqliteHarbor,
   name: MyDatabases,
   storage: {SqliteHarbor.Storage.S3,
     endpoint: "https://ACCOUNT_ID.r2.cloudflarestorage.com",
     bucket: "my-databases", region: "auto",
     access_key_id: System.fetch_env!("R2_ACCESS_KEY_ID"),
     secret_access_key: System.fetch_env!("R2_SECRET_ACCESS_KEY")},
   prefix: "app/",
   cache_dir: "/data/sqlite-harbor"}
]

{:ok, id} = SqliteHarbor.create(MyDatabases)
{:ok, results} = SqliteHarbor.execute(MyDatabases, id, [
  "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)",
  {"INSERT INTO notes(body) VALUES (?)", ["hello"]},
  "SELECT * FROM notes"
])
```

For local development, use `storage: {SqliteHarbor.Storage.Local, root: "/tmp/objects"}`.
Local serializes conditions within one BEAM node; it is **single-node only**.
Every cluster node needs the same instance name, storage and prefix, with its own
exclusive cache directory. Discover/connect nodes in your host application. Kernel
`connect_all` and `prevent_overlapping_partitions` must both be true (checked at start).
Synchronize host clocks: deadlines cross nodes as wall-clock expiries, then become
local monotonic deadlines. The caller independently enforces its monotonic budget.
The prefix is empty or ends with `/`, using ASCII letters, digits and `._~/-` only.

## API

```elixir
SqliteHarbor.create(service)                         # {:ok, uuid}
SqliteHarbor.delete(service, uuid)                   # :ok, also when already absent
SqliteHarbor.list(service, nil, 100)                 # {:ok, [uuid], next_cursor}
SqliteHarbor.execute(service, uuid, statements, timeout: 5_000)
SqliteHarbor.execute(service, uuid, statements, row_limit: :truncate)
SqliteHarbor.export(service, uuid)                   # {:ok, binary_chunk_stream}
SqliteHarbor.import(service, path_or_chunk_stream)   # {:ok, new_uuid}
```

Non-execute storage and validation failures return `{:error, reason}`. Execute errors are
`{:error, %{statement: zero_based_index_or_nil, code: atom, message: text}}`.
Each result has `columns`, `rows`, `changes`, `last_insert_rowid`, and `truncated`. Changes count
the statement's direct and trigger writes; reads and DDL report zero. The row ID is
SQLite's most recent successful insert on that connection.
Params are lists or maps with SQLite parameter names as binary keys (such as `":id"`).
Values are integer, float, text binary, `{:blob, binary}` or `nil`. Text and BLOB results
are both returned as binaries, as in exqlite; output blob tagging is deferred.

**A list is one transaction.** The first failure rolls it back. Each SQL string must
contain exactly one statement; triggers can contain their normal SQL body. BEGIN,
COMMIT, ROLLBACK, SAVEPOINT, ATTACH, DETACH and TEMP objects are refused
through SQLite's authorizer. Schema inspection also accepts `PRAGMA table_info('table')`,
`table_xinfo`, `index_list`, `index_info`, and `foreign_key_list`, with the same
parenthesized table/index argument. The argument can be a bare identifier or a quoted
name (including escaped quotes); optional `main.`, surrounding whitespace and a trailing
semicolon are accepted. Other PRAGMAs, assignment syntax,
and table-valued PRAGMAs remain refused. Comments inside or following an otherwise
allowed PRAGMA are refused as well. `sqlite_schema` is also available.
VACUUM (including INTO) fails inside the transaction. Extensions
are disabled. `trusted_schema=OFF` applies to every connection.

Export activates the owner, adopts any local crash leftovers, saves, then downloads
and verifies one consistent object version. Enumerate its stream once; enumeration
completion or early halt deletes the temporary file. An abandoned stream is cleaned
on the next node boot. Import validates the header, size and SQLite quick_check
on a read-only scratch connection, then stores it under a fresh ID. A binary import
source is a trusted local filesystem path, so pass tenant upload bytes as a chunk stream.
Delete and the export-save phase each have a fixed 30-second caller timeout. A timeout
or storage transport error can leave an operation's outcome unknown; deletion is
idempotent, and repeating it confirms absence. Queued execute requests expire without
running, but a commit already in progress may complete at the deadline.

By default, exceeding `max_rows` fails with `:result_too_large` and rolls back the
transaction. Pass `row_limit: :truncate` to return at most `max_rows` rows across the
whole call and set `truncated: true` on each statement that omitted rows. Statements
still run to completion, including writes with `RETURNING` and later statements, then
commit. An exact fit has `truncated: false`. Discarded rows do not consume the returned
byte budget, but each discarded row remains bounded by `max_result_bytes` before it
is copied from SQLite. Value limits, returned byte limits, deadlines and SQL errors
still fail and roll back the whole transaction.
An unsupported `row_limit` value returns `:invalid_row_limit` before execution.

Delete removes the primary object, every current object under `conflicts/ID/`, and
ID-specific database files, sidecars, metadata, snapshots, conflicts and download/export
scratch files on connected caches. It coordinates with conflict uploads and returns an
error if any purge step fails; repeat delete to finish a partial purge. Large purges
can exceed the fixed caller timeout while continuing in the owner. All cluster nodes
must run the same library version.

## Durability and limits

Success from execute means SQLite committed **locally**. Dirty databases save whole
files every two minutes and on idle/shutdown. While storage is healthy, a node lost
with its disk can lose one save interval plus an upload (roughly 2–3 minutes at the
defaults). During an outage, everything since the last successful save is exposed.
If the disk returns, dirty files and WALs are recovered. SQLite NORMAL mode can lose
recent writes on an OS crash or power loss, even if the disk survives.

Cluster ownership uses `:global`, with conditional object PUTs as protection against
overwrites. On conflicts, the loser's latest file goes to `conflicts/` for a human;
its writes are absent from the primary database. An already-sent PUT can land after
owner death or name resolution; a replacement can then conflict too. There is no
bucket lease or strict fencing. See [architecture](docs/architecture.md).

| Setting | Default |
| --- | --- |
| `max_db_bytes` | 100,000,000 |
| `save_interval` | 120,000 ms |
| `idle_timeout` | 300,000 ms |
| `cache_budget_bytes` (clean idle files) | 1,000,000,000 |
| `max_statements` | 100 per call |
| `max_request_bytes` (SQL + params) | 1,000,000 |
| `max_rows` | 10,000 per call |
| `max_result_bytes` | 10,000,000 per call |
| `max_value_bytes` | 10,000,000; SQLite string/BLOB/encoded-row length limit |
| `wall_timeout` | 10,000 ms, including queue time |
| execute `timeout` option | 30,000 ms; effective budget is the smaller limit |
| `hard_heap_limit` | 1,000,000,000 bytes, **whole OS process** |
| `shutdown_timeout` | 120,000 ms per owner; size for your workload |
| `recheck_interval` | 30,000 ms, leftovers/conflicts/clean eviction |
| `retry_max` | 30,000 ms; failed upload retries start at 2,000 ms and double |

SQLite's heap limit includes **all SQLite connections in the BEAM**, including the
host's own, and their page caches (roughly 2 MB per active connection). A lower existing
hard limit wins; SQLite does not raise an already-set limit. One tenant exhausting
the shared budget can make other connections return out-of-memory. This library is
not an isolation boundary for hostile workloads. FTS shadow-table self-corruption is
accepted; files and SQL are not protected from an authorized caller's own changes.
The per-connection length limit also bounds intermediate values and bound parameters,
before the NIF copies them into BEAM memory. Oversized values fail with `:too_big`.
Results are checked one row at a time before the following row is evaluated, with the
remaining aggregate byte budget enforced inside the NIF before any cell copies.
SQL sorters can spill to the OS temporary directory within the execution deadline;
the database page cap does not bound those temporary spill files.
Disconnected/offline disks cannot be erased until they return: missing primary objects
cause dirty database recovery and conflict sweeps to discard old copies; clean cached
databases are removed at activation or eviction. An already-sent upload or download can
still land after delete, so retry deletion after reconnecting nodes and settling such
requests. Bucket version history, replicas, external exports and backups require the
host's own retention/deletion policy. Configure a stable bucket and prefix: a missing
primary means deletion, and the sweep discards local conflicts with a
`:conflict_discarded` telemetry event.

The disk must hold active databases, WAL/temp files and one immutable snapshot per
saving owner, plus clean cache and pending conflict rescues. Dirty files are never
evicted. The page cap prevents further growth; a cap lower than an existing database
does not shrink it. Cold large databases may take several seconds to download. A
subsecond warm first query is a target, not a measured guarantee; parallel ranges are
not implemented. [Operations and telemetry](docs/operations.md) describe recovery.

## Verify your bucket

Create a throwaway bucket yourself, then supply its name and a private JSON file:

```sh
mix run scripts/check_r2.exs TEMP_BUCKET /private/path/r2-credentials.json
```

The file has `access_key_id`, `secret_access_key` and `endpoint` (your account's S3 API
endpoint), optionally `region` (default `"auto"`). The script prints only
success/failure labels, creates uniquely prefixed objects, and deletes only those
objects afterwards. It does not create or delete buckets. Delete your empty temporary
bucket after the probe. Never paste credentials in a terminal command or commit them.

## Development

```sh
mix deps.get
mix format --check-formatted
mix test --warnings-as-errors
```

The default suite includes lifecycle failures and two real BEAM peers, using a central
faithful Local store to preserve atomic conditional writes across peers. It does not
use R2 credentials. See [CONTRIBUTING](CONTRIBUTING.md) and [SECURITY](SECURITY.md).

MIT licensed. Object-storage request patterns draw on
[Guardian](https://github.com/pguillory/guardian). exqlite supplies SQLite; denoland/celld
was design prior art only.
