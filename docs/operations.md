# Operations

Before first adoption, run scripts/check_r2.exs against real storage. This repository
has not yet verified R2 conditional writes because credentials were unavailable when
it was built. Do not infer that passing Local tests qualifies another S3 implementation.
The probe covers If-None-Match, current/stale If-Match, conditions on a missing object,
MD5 ETags, conditional GET, and idempotent DELETE.

Mount a writable, durable cache directory exclusively for each node/instance, with
enough space for active files, snapshots, WAL/temp work, clean cache and unsent rescues.
Use one bucket prefix only with matching cluster service names/configuration. Do not
edit, expire, multipart-upload or overwrite library primary objects out of band. Preserve
conflict objects for human review; recovery does not merge databases.

At deployment, let the supervision tree shut down normally. Configure shutdown_timeout
to cover the workload's dirty bytes divided by measured upload bandwidth; the default
120 seconds is a starting point, not a capacity guarantee. The outer supervisor/host
shutdown budget must also allow its children to complete. Storage outages can prevent
a clean drain; killing the process then accepts the documented local durability risk.
Synchronize clocks on every node for cross-node execute deadlines. Configure BEAM
distribution securely; anyone with its cookie and network access is trusted.

## Telemetry

Events are `[:sqlite_harbor, EVENT]`. Attach handlers in the host. No alert service is
built in. Metadata includes service/id and outcome/reason where applicable; SQL and
parameter values are not emitted. Measures include duration_ms or bytes where relevant.

| Event | Meaning |
| --- | --- |
| registration | Cluster-name acquisition latency |
| activation | Download/open, with cache hit/miss/stale |
| execute | Local execution result and time |
| save | One snapshot upload attempt, bytes/time/outcome |
| unsaved | An owner could not complete its save |
| conflict | Latest losing file quarantined; **alert** |
| adoption | Dirty file reopened after interruption |
| adoption_stranded | Another node owns a database with local dirty leftovers |
| adoption_failed | Recovery owner crashed again; **alert** |
| cache_task_failed | Background recovery/upload task failed |
| eviction | Clean unused cache bytes removed |

Keep runtime crash logging enabled. Repeated unsaved events, adoption failures and
conflict events require investigation. A node whose disk is lost can lose everything
since its last successful upload; an outage stretches that window. A frozen/partitioned
node may keep its names until distribution failure detection (normally 45–75 seconds).

## Manual checks still pending

- Real R2 probe in a throwaway bucket, followed by bucket deletion.
- Production-host cold/warm first-query and 100 MB throughput measurement.
- Production shutdown budget sizing and OS-crash/power-loss durability validation.
- A real network-partition/heal exercise; automated peers cover routing/failover and
  resolver notification, with faithful storage conflicts exercised separately.

## Deployment notes

No migration or backfill. The host supplies storage credentials, a cache mount, shared
name/prefix, discovery, clock synchronization, telemetry handlers, and a suitable shutdown
budget. Verify bucket conditions before adoption, then create/write/export/import/delete
one throwaway database. Check conflict/unsaved/adoption_failed events remain absent during
normal use. SqliteHarbor itself does not deploy anything.
