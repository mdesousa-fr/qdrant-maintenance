# qdrant-maintenance

A Go CLI for listing Qdrant collections, backing up distributed Qdrant nodes to
S3, and pruning stored snapshots, intended exclusively for cronjobs and Kubernetes
CronJobs. All command results, help, errors, and diagnostics are newline-delimited
JSON logs on stderr. stdout stays empty. OpenTelemetry exports remain available.

## Build and test

Requires Go 1.26 or newer.

```sh
go build -o bin/qdrant-maintenance ./cmd/qdrant-maintenance
go test ./...
go test -race ./...
go vet ./...
```

Tests run entirely offline: Qdrant handlers execute through an in-memory HTTP
transport, S3 SDK operations are mocked (including multipart upload and abort),
and OTLP exporters use an in-memory collector transport. No listening sockets,
AWS credentials, Docker, or running services are needed. Dependencies must be
available in the Go module cache before running offline.

## Configuration

Copy the relevant settings from [.example.env](.example.env). The CLI reads the
process environment; it does not automatically load `.env` files.

| Variable | Purpose / default |
| --- | --- |
| `QDRANT_NODES` | JSON object mapping stable node IDs to **direct node REST endpoints**. Required for backups. |
| `QDRANT_URL` | Optional endpoint for listing collections. Defaults to the first configured node. Never used as a backup fallback. |
| `QDRANT_API_KEY` | Optional API key; configure it when authentication is enabled. |
| `QDRANT_TIMEOUT` | Positive Go duration per Qdrant HTTP request, including snapshot creation/download; default `30m`. |
| `S3_BUCKET` | Required for snapshot listing, creation, and retention. Use a dedicated bucket for one cluster. |
| `S3_ENDPOINT` | Optional HTTP(S) endpoint for S3-compatible storage. Empty uses AWS's regional endpoint. |
| `S3_FORCE_PATH_STYLE` | Boolean; defaults to `true` for custom endpoints and `false` for AWS. |
| `AWS_REGION` | AWS region; falls back to `AWS_DEFAULT_REGION` and the SDK configuration chain. |
| `S3_TIMEOUT` | Positive duration per storage operation, including upload and verification; default `30m`. |
| `RETENTION_KEEP_LAST` | Nonnegative count retained per collection/node; default `5`. |
| `LOG_LEVEL` | Case-insensitive `DEBUG`, `INFO` (default), `WARN`, or `ERROR`. Applies to stderr and OpenTelemetry logs. |
| `TMPDIR` | Directory for temporary snapshot files; defaults to the OS temporary directory. |

AWS credentials use the standard SDK chain, including workload identity, web
identity, container credentials, shared configuration, and environment variables.
S3-only commands do not require Qdrant settings. Listing and backup commands do
not depend on retention settings. Help and version do not need Qdrant or S3.

Example for a StatefulSet with three nodes:

```sh
export QDRANT_NODES='{"qdrant-0":"http://qdrant-0.qdrant-headless:6333","qdrant-1":"http://qdrant-1.qdrant-headless:6333","qdrant-2":"http://qdrant-2.qdrant-headless:6333"}'
export QDRANT_API_KEY='your-api-key'
export S3_BUCKET='your-cluster-snapshots'
export AWS_REGION='eu-west-1'
```

Configure **every node**, keep the IDs stable, and use pod-specific/headless DNS
addresses. The CLI validates distinct peer identities, complete cluster membership, and
consistent collection inventories at every configured endpoint before creating snapshots.
Membership and collection inventory are checked again before reporting completion. Load-balanced services are unsuitable because
creation, download, and deletion must reach the same node.

Qdrant collection snapshots contain only the data on the node that creates them.
A collection backup therefore needs all its nodes' snapshots. Sequential node
snapshots are not a cluster-wide atomic point-in-time backup, and collection
aliases are not included. See [Qdrant's snapshot documentation](https://qdrant.tech/documentation/snapshots/).
Restore orchestration and alias backup are outside this CLI's current scope.

## Commands

```sh
qdrant-maintenance collections

# List node snapshots in S3, newest first.
qdrant-maintenance snapshots list documents

# Back up this collection on every configured node.
qdrant-maintenance snapshots create documents

# Back up all collections on every configured node.
qdrant-maintenance snapshots create --all

# Keep the Qdrant copies after a successful transfer.
qdrant-maintenance snapshots create --keep-source documents

# Preview retention before applying it.
qdrant-maintenance retention plan
qdrant-maintenance retention apply

# Explicitly allow removing all recognized snapshots.
RETENTION_KEEP_LAST=0 qdrant-maintenance retention apply --force
```

Place action flags such as `--all` and `--keep-source` before collection names.
There is no separate result-output mode; the former `--json` option has been
removed. Every stderr line has the standard logger fields (`time`, `level`, `msg`)
and event-specific attributes. `help`, `--help`, version information, and flag
validation errors also use structured logs.

At INFO level, listing emits one `snapshot found` event per snapshot, with `name`,
`key`, `collection`, `node`, `last_modified`, and `size`. A `snapshots listed`
summary includes `snapshot_count`, excluding unrelated objects and other
collections. An empty listing logs `snapshot_count: 0`. Collection listing uses
`collection found` events and a `collection_count` summary. Retention logs its
plan and selected keys, and logs completion after successful application.

`LOG_LEVEL` applies to all command results and diagnostics. For example, `ERROR`
suppresses successful listings. For troubleshooting, DEBUG logs include raw S3
keys:

```sh
LOG_LEVEL=DEBUG qdrant-maintenance snapshots list documents
```

Only the documented snapshot key layouts are recognized, and the collection name
must match exactly. To combine process streams for a collector, `2>&1` still
produces only JSON log records because stdout is empty.

### Backup safety and failures

For each node and collection, the CLI:

1. Creates a Qdrant snapshot synchronously.
2. Streams its download into a private temporary file and computes SHA-256.
3. Validates the byte count and Qdrant checksum, when supplied.
4. Uploads to `<collection>/<node>/<snapshot-name>.snapshot`, using multipart
   transfers for large files, two workers, and 16 MiB parts. The AWS SDK provides
   request retries and transport checksums.
5. Checks the committed object's size and SHA-256 metadata with `HeadObject`.
6. Deletes the Qdrant copy, unless `--keep-source` is set.

Uploads use conditional creation to avoid overwriting an existing backup key. A
key collision is reported as a failure, and the Qdrant copy is retained.

Temporary files are removed on return, including errors. Provision temporary disk
space for the largest individual snapshot. Memory does not grow with snapshot
size. Interrupted multipart uploads are aborted on a best-effort basis; an S3
lifecycle rule for incomplete multipart uploads handles abrupt pod termination.

Failed downloads, checksum validation, uploads, or S3 verification retain the
Qdrant snapshot. A cleanup failure reports that the backup is stored while
returning a failure status. Backup attempts continue for other collections/nodes
after ordinary failures. SIGTERM/SIGINT cancels outstanding work and stops new
attempts. Re-running a failed command creates fresh snapshots; it does not resume
previous Qdrant snapshots or multipart uploads. Failed source snapshots can be
removed manually after investigation.

Exit status is `0` for success and `1` for configuration, operation, cancellation,
or telemetry shutdown errors. Each backup attempt emits a `backup completed` or
`backup failed` event with `node`, `collection`, `key`, `size`, `sha256`, `stored`,
and `source_deleted`; failures include `error`. Partial failures produce a nonzero
status. Inspect `stored` and `error`; a key in a failed event does not by itself
prove the backup was committed and verified.

### Retention

Retention and listing recognize only `<collection>/<node>/<filename>.snapshot`
keys with a nonempty node segment. Objects without a node segment, other object
layouts, and other file extensions are ignored and are not eligible for retention
deletion.

The newest `RETENTION_KEEP_LAST` objects are retained independently for every
collection/node, ordered by S3 `LastModified`, with the key as a deterministic
tie-breaker. Plans are deterministic. Zero requires `apply --force`; negative
values are rejected even for an empty bucket. Deletion failures are aggregated
while remaining objects are attempted, and produce a nonzero status. Cancellation
stops further deletes.

Backup does not run retention automatically. Schedule pruning separately or run
it only after a successful backup in your job orchestrator. Prevent overlapping
backup jobs and topology changes during a backup window. `retention apply` builds
a fresh plan from the bucket; it does not consume a previously printed plan.
In versioned S3 buckets, ordinary deletions create delete markers; noncurrent
versions require bucket lifecycle rules to reclaim their storage.

## OpenTelemetry

Logs, traces, and metrics export through **OTLP HTTP/protobuf**, suitable for
short-lived jobs. Configure a Collector endpoint to enable all three signals:

```sh
export OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318
export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
export OTEL_SERVICE_NAME=qdrant-maintenance
export OTEL_RESOURCE_ATTRIBUTES=deployment.environment.name=production
qdrant-maintenance snapshots create --all
```

Common and per-signal OTLP endpoint, TLS, header, timeout, and compression settings
are handled by the official exporters. Per-signal endpoints include their signal
path (for example `/v1/traces`). `OTEL_TRACES_EXPORTER`, `OTEL_METRICS_EXPORTER`,
and `OTEL_LOGS_EXPORTER` accept `otlp` or `none`. Explicit `otlp` enables a signal
even without a configured endpoint, using the exporter's localhost default.
`OTEL_SDK_DISABLED=true` disables all export. Without an endpoint or explicit
exporter, no Collector connection is attempted. gRPC export is not supported.

Command, backup, S3, retention-apply, and outbound HTTP spans carry operation
outcomes. Logs emitted with the active context carry trace/span IDs in OTLP.
Metrics include:

- `qdrant_maintenance.operations`: count by operation and outcome.
- `qdrant_maintenance.operation.duration`: seconds by operation and outcome.
- `qdrant_maintenance.backup.bytes`: bytes uploaded and verified.
- `qdrant_maintenance.retention.deleted`: successful retention deletions.

Collection names, node IDs, and object keys are excluded from custom metric
attributes to bound cardinality. They appear in appropriate spans/logs. All
providers shut down before process exit using a fresh context with a 10-second
budget, including failure and cancellation paths. Local JSON logs remain on
stderr whether exporting is enabled or disabled. AWS SDK and OpenTelemetry diagnostics also use structured JSON logs.
Exporter diagnostics stay local to avoid recursive OTLP failures. Export failures are reported;
shutdown export failures produce a nonzero exit status.

See [OpenTelemetry's Go exporter documentation](https://opentelemetry.io/docs/languages/go/exporters/).

## Kubernetes and containers

```sh
docker build -t your-registry/qdrant-maintenance:1.0.0 .
```

[deploy/cronjob.yaml](deploy/cronjob.yaml) runs the binary as non-root with a
read-only root filesystem, writable disk-backed temporary storage, SIGTERM grace,
and `concurrencyPolicy: Forbid`. Replace the image, endpoints, bucket, and API-key
Secret. Create the referenced ServiceAccount and configure its cloud workload
identity. Adjust disk space, memory, and the total job deadline for your snapshot
sizes and collection/node count. Retries can create extra snapshots.

[deploy/retention-cronjob.yaml](deploy/retention-cronjob.yaml) provides a separate
S3-only pruning job. `Forbid` prevents overlap within each CronJob; coordinate
manual runs and the two schedules through your orchestrator when necessary.

[deploy/otel-collector.yaml](deploy/otel-collector.yaml) is a Collector configuration
example for all three signals; connect its pipelines to your production backends.
The CLI pushes telemetry and does not expose a long-lived HTTP metrics server.

The existing `docker-compose.yaml` and `scripts/init-snapshots.sh` are optional
manual development fixtures. Their seeded S3 objects are placeholder files, not
restorable Qdrant backups. They are not used by the unit test suite.

## Operational event schema

Command events include a stable `event` identifier and a `run_id`. Backup runs
emit `backup.run_summary` with expected, successful, failed, skipped, and source
cleanup failure counts, uploaded bytes, duration, outcome, and completeness.
Identifiers are log attributes, never custom metric labels.

Cluster validation fails before writes for missing peers, duplicate endpoints,
unreachable nodes, or inconsistent inventories. Membership or collection changes
during a run prevent completion. This checks cluster membership and collection
names; it does not freeze writes or shard placement during the backup window.

Manifests use schema version 1 and live at `runs/<run_id>.json`, outside the
snapshot key layout. Their writes use S3 conditional creation and ETag updates;
unsupported or inconsistent manifests fail closed instead of driving deletion.

### Durable backup progress

Every new backup creates its manifest before creating any Qdrant snapshots.
Snapshot identity is saved after creation, and successful upload metadata is
saved before source deletion. A manifest-write failure stops further side effects.
A process killed between Qdrant creation and recording its returned name can
still leave an unrecorded source snapshot; inspect its logs before manual cleanup.
`runs list` logs manifest state and timestamps without requiring Qdrant access.
Backup completeness is independent of source-cleanup success. A fully stored run
can be `complete` while reporting cleanup errors and returning a nonzero status.
