# Architecture

Each named instance supervises Cache, the DynamicSupervisor for owners, and Drain in
that order with rest_for_one. Owners are temporary children. Drain runs first on normal
shutdown, refusing new local checkouts while owners finish saves with their names held.
When Cache fails, existing watchdogs kill their owners outright. Owners also check
their original Cache pid before graceful termination. The supervisor waits for all old
owners to die before the new Cache starts recovering files.

Only a registered owner's Cache checkout permits access to that database's local files.
Clean unheld files can be evicted. Cache monitors holders and recovers dirty files after
owner crashes. If that recovery owner also crashes, automatic recovery stops for that
database and emits adoption_failed; an explicit new caller can try again. A periodic
recheck revisits dirty leftovers that could not register because another node won.
Slow recovery starts and conflict uploads are tasks linked to Cache.

The owner registers `{SqliteHarbor, service, id}` through :global, then downloads in
handle_continue, leaving its DynamicSupervisor responsive. The reconnect resolver keeps
one pid registered and notifies both owners. The losing owner stops serving and rescues
its latest state. The kept owner tries a fresh conditional save. The API retries only
normal/shutdown/noproc call exits, where a queued request did not run, against a different
owner within its deadline. Abnormal exits return unknown_outcome.

## On disk

```
cache_dir/
  db/ID.sqlite          # plus SQLite -wal and -shm
  db/ID.meta            # base ETag, pending MD5, clean/dirty
  snapshots/ID.MD5      # immutable pending upload
  conflicts/ID--STAMP   # latest losing file, retained until uploaded
  tmp/                 # import/export scratch files
```

Metadata uses an fsynced temporary file, rename and directory fsync. Database/snapshot
copies and renames are fsynced too. A missing database makes its metadata irrelevant;
its referenced snapshot is removed at boot. Unreferenced snapshots and scratch files
are removed at node boot. A per-directory VM-lifetime marker preserves live callers'
scratch files across Cache restarts in the same VM. Cache directories are exclusively owned by one live instance and
must be private to the host operator.

Primary objects are `prefix/db/ID`; rescues are `prefix/conflicts/ID/UTC-NODE-SUFFIX`.
Objects are single-part uploads. The ETag must be the body's lowercase MD5 hex.
Backends implement GET-to-file (If-None-Match), HEAD, conditional PUT-from-file with
Content-MD5, idempotent DELETE, and paginated LIST. S3 uses ReqS3 with explicit content
length, disabled redirects/compression/retries, and HTTP timeouts. Local is a faithful
single-node implementation, not a cluster store.

## Saving and recovery

WAL growth bumps a generation, including conservatively on failed calls that spill
pages. At 4 MiB the owner checkpoints between calls; every prepared statement is
released. A save checkpoints fully (busy retries after 3 seconds), copies the main file
into a private fsynced snapshot, records its MD5 in dirty metadata, then starts a linked
upload task with If-Match on the base ETag. Calls can commit while that snapshot uploads.
The saved generation advances only to that snapshot's generation.

Timeouts/errors retain the same pending snapshot and condition. A 412/404 triggers HEAD:
pending MD5 means our earlier attempt succeeded; 404 means deleted; any other version
means conflict. Successful outcomes update metadata before removing the snapshot.
Conflict checkpoints the latest live state, closes, moves it to conflicts/, removes
metadata, then removes the snapshot. Cache deletes rescues only after object storage
accepts them (or HEAD proves an earlier rescue upload landed).

Activation revalidates a clean file with one conditional GET. Dirty activation HEADs the
object, adopts base/pending matches and replays any unknown pending upload before making
a new snapshot. Replayed generations are unknown, so resolution always leaves the file
dirty and forces a fresh save containing subsequent WAL commits. Missing referenced
snapshots or a newer remote version quarantine local data before downloading the winner.
Missing remote databases discard leftovers. IDs are UUIDv4 and never reused.

Idle check-in stops serving while it waits for saves, closes, writes clean metadata and
checks in. Failure resumes service with dirty metadata. Delete holds ownership through
the remote DELETE and only then removes local files. Across a partition, an already-sent
request can still land after its task dies. A replacement may then conflict; these
windows, whole-node/disk loss, storage outages and netsplit loss are explicit tradeoffs.

## Execution

Every request is a transaction with SQLite's authorizer installed through preparation
and stepping. Internal transaction/checkpoint operations temporarily lift it. Strict
preparation rejects extra SQL without compiling a rejected tail (some PRAGMAs take effect
during preparation). Input limits run before dispatch; row/byte limits are cumulative
through the batch. Single-row stepping passes the remaining byte budget to the NIF,
which sums column sizes before copying any cells, including repeated shared values.
Every tenant connection also
sets SQLite's string/BLOB/encoded-row length limit (default 10 MB), rejecting oversized
generated/intermediate values before copying them into BEAM memory. A linked watchdog monitors Cache and interrupts overdue statements
every 50ms until synchronously disarmed. The owner also checks the local deadline before
prepare, row chunks and commit; a late cancellation cannot affect the next call.

Read-only scratch validation preserves WAL header bytes and cleans any coordination
sidecars. Export first flushes the owner, then checks a single downloaded object's body
against its ETag. It retries an integrity failure once. There are no ranged downloads,
storage leases, migrations, network servers or node-wide execution semaphores.
