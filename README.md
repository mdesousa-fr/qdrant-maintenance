# qdrant-maintenance

A Go CLI for listing Qdrant collections, backing up distributed Qdrant nodes to
S3, and pruning stored snapshots, intended exclusively for cronjobs and Kubernetes
CronJobs. All command results, help, errors, and diagnostics are newline-delimited
JSON logs on stderr. stdout stays empty. OpenTelemetry exports remain available.

## Quickstart

### Docker image

Download the versioned image from [GitHub Container Registry](https://github.com/users/mdesousa-fr/packages/container/package/qdrant-maintenance):

```sh
docker pull ghcr.io/mdesousa-fr/qdrant-maintenance:1.0.0
docker run --rm ghcr.io/mdesousa-fr/qdrant-maintenance:1.0.0 help
docker run --rm ghcr.io/mdesousa-fr/qdrant-maintenance:1.0.0 version
```

Docker automatically selects the Linux AMD64 or ARM64 image for your platform.
To run commands against your services, put the settings described in
[Configuration](#configuration) in an `.env` file and pass it to the container:

```sh
docker run --rm --env-file .env \
  ghcr.io/mdesousa-fr/qdrant-maintenance:1.0.0 collections
```

Service URLs must be reachable from inside the container.

### Download the CLI

Precompiled **Linux** binaries for AMD64 (x86-64) and ARM64 (aarch64) are available
in [GitHub Releases](https://github.com/mdesousa-fr/qdrant-maintenance/releases).
The commands below use version `1.0.0`; change `VERSION` to select another release.
Run them on the Linux host where you will use the CLI.

First, select the archive for your architecture:

```sh
VERSION=1.0.0
case "$(uname -m)" in
  x86_64) ARCH=amd64 ;;
  aarch64|arm64) ARCH=arm64 ;;
  *) echo "Unsupported architecture: $(uname -m)" >&2; exit 1 ;;
esac
ARCHIVE="qdrant-maintenance_${VERSION}_linux_${ARCH}.tar.gz"
RELEASE_URL="https://github.com/mdesousa-fr/qdrant-maintenance/releases/download/v${VERSION}"
```

Download with **curl**:

```sh
curl --fail --location --output "$ARCHIVE" "$RELEASE_URL/$ARCHIVE"
```

Or download with **wget**:

```sh
wget --output-document="$ARCHIVE" "$RELEASE_URL/$ARCHIVE"
```

Extract with **tar**, install in your user's binary directory, and check the version:

```sh
mkdir -p "$HOME/.local/bin"
tar -xzf "$ARCHIVE" -C "$HOME/.local/bin" qdrant-maintenance
export PATH="$HOME/.local/bin:$PATH"
qdrant-maintenance version
qdrant-maintenance help
```

Add `$HOME/.local/bin` to your shell's startup `PATH` to keep the CLI available in
new terminals. Configure the environment variables in [Configuration](#configuration)
before using commands that connect to Qdrant or S3. Help and version work without
service configuration.

## Build and test

Requires Go 1.27.3 or newer (patched toolchain).

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
| `QDRANT_PARALLEL_NODES` | Maximum concurrent nodes for `snapshots create` and `runs retry`; positive integer, default `1`. Explicit `--parallel-nodes` takes precedence. |
| `S3_UPLOAD_CONCURRENCY` | Positive multipart worker count per upload; default `2`. |
| `S3_UPLOAD_PART_SIZE_MIB` | Multipart part size in MiB, integer from `5` to `5120`; default `16`. The SDK may increase parts for large files. |
| `S3_BUCKET` | Required for snapshot listing, creation, and retention. Use a dedicated bucket for one cluster. |
| `S3_ENDPOINT` | Optional HTTP(S) endpoint for S3-compatible storage. Empty uses AWS's regional endpoint. |
| `S3_FORCE_PATH_STYLE` | Boolean; defaults to `true` for custom endpoints and `false` for AWS. |
| `S3_INSECURE_SKIP_VERIFY` | Boolean; default `false`. Set to `true` to skip certificate and hostname verification for all S3 operations. Applies only to S3; use for trusted endpoints with self-signed certificates. |
| `AWS_REGION` | AWS region; falls back to `AWS_DEFAULT_REGION` and the SDK configuration chain. |
| `S3_TIMEOUT` | Positive duration per storage operation, including upload and verification; default `30m`. |
| `RETENTION_POLICY` | `snapshot` (default) or `run` for complete manifest-backed sets. |
| `RETENTION_KEEP_LAST` | Nonnegative count per collection/node (`snapshot`) or complete runs per coverage group (`run`); default `5`. |
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
A collection backup therefore needs all its nodes' snapshots. Node snapshots, including parallel snapshots, are not a cluster-wide atomic point-in-time backup, and collection
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
   transfers for large files, configurable workers (default two), and configurable
   parts (default 16 MiB). The AWS SDK provides
   request retries and transport checksums.
5. Checks the committed object's size and SHA-256 metadata with `HeadObject`.
6. Deletes the Qdrant copy, unless `--keep-source` is set.

Uploads use conditional creation to avoid overwriting an existing backup key. A
key collision is reported as a failure, and the Qdrant copy is retained.

Temporary files are removed on return, including errors. Provision temporary disk
space for the sum of the largest simultaneous snapshots, plus a safety margin. Memory does not grow with snapshot
size. Interrupted multipart uploads are aborted on a best-effort basis; an S3
lifecycle rule for incomplete multipart uploads handles abrupt pod termination.

Failed downloads, checksum validation, uploads, or S3 verification retain the
Qdrant snapshot. A cleanup failure reports that the backup is stored while
returning a failure status. Backup attempts continue for other collections/nodes
after ordinary failures. SIGTERM/SIGINT cancels outstanding work and stops new
attempts. Re-running `snapshots create` starts a fresh run; `runs retry <run_id>`
resumes recorded source snapshots. Multipart uploads themselves are restarted.
Unrecorded source snapshots require investigation before manual cleanup.

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
- `qdrant_maintenance.backup.bytes`: bytes processed through successful transfer
  verification, including matching destinations accepted during recovery.
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

Command events include a stable `event` identifier and an `execution_id`. Backup
creation and recovery events also carry the persistent `run_id`. Backup runs
emit `backup.run_summary` with expected, successful, failed, skipped, and source
cleanup failure counts, stored bytes, duration, outcome, and completeness. Retry
and cleanup commands emit the corresponding summary events as well.
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

### Backup freshness

`runs check --max-age 26h` validates current cluster coverage, finds the latest
complete whole-cluster run, and checks its age and required S3 object metadata.
Use `--collection documents` to check that collection across all peers; a complete
whole-cluster run can satisfy that check. Missing objects, recorded content
verification failures, stale runs, or absent coverage return a nonzero status.
The command does not fall back to an older run when the latest qualifying run is
damaged. `qdrant_maintenance.backup.age` records the healthy run's age in seconds.

[deploy/freshness-cronjob.yaml](deploy/freshness-cronjob.yaml) runs this check
independently, so a backup job that never starts still becomes detectable. Alert
on failed or missing freshness-check Jobs as well as backup failures. Checks
require access to Qdrant for current membership and S3 for durable state.

### Retention policy selection

`RETENTION_POLICY=snapshot` remains the default. The optional `run` policy groups
complete runs by cluster peer membership and collection scope, keeping the newest
`RETENTION_KEEP_LAST` complete runs in each group. Incomplete, active, and known
corrupt runs remain protected and do not replace healthy retained runs. Objects
without manifests are unmanaged by run retention. Different historical peer sets
remain separate groups so a topology change does not silently discard their sets.

Use `RETENTION_POLICY=run qdrant-maintenance retention plan` before applying the
policy. Apply verifies retained runs' objects, reserves candidates with conditional
manifest writes, and rechecks manifest revisions before deletion. Shared keys
referenced by protected runs are preserved. Deletion progress is checkpointed;
partial failures remain `pruning` and can be retried by rebuilding/applying a plan.
Do not mix snapshot and run retention jobs against the same backup history:
snapshot retention can remove members of otherwise complete runs.

Incomplete runs never expire automatically. Preview with
`runs expire --older-than 168h`, then add `--apply` to remove their verified S3
uploads. Complete runs, other protected references, unmanaged objects, unverified
uploads, and Qdrant sources are not deleted by expiry. Expired/pruned manifests
remain as audit records. An interrupted process may leave a writer reservation;
use `--takeover` on apply/expiry only after ensuring the previous process has
terminated. ETag checks still reject concurrent state changes.

### Retrying recorded transfers

`runs retry <run_id>` verifies current peer/collection coverage and resumes the
recorded run. Known Qdrant snapshot names are reused; a missing named source is an
error and is never silently replaced. Entries that failed before obtaining a
snapshot name can create their first recorded snapshot. Already-verified S3 objects
are reused. If an upload succeeded before its checkpoint failed, retry computes
the source hash and can accept the matching existing object without overwriting it.
Conflicting destination content is never overwritten. A recorded content integrity
failure must be resolved by full content verification before retry can trust it.

Reservations detect competing recovery processes. After confirming an interrupted
writer has terminated, `runs retry --takeover <run_id>` can acquire its reservation
with an ETag check. A run's original start time is preserved. Freshness and run
retention use run start time so retrying old data cannot make it appear new.

`runs cleanup <run_id>` explicitly cleans recorded source snapshots, including
runs created with `--keep-source`. Each candidate must still have a healthy,
verified S3 copy; absent/unverified copies are never a basis for Qdrant deletion.
Already-cleaned sources are skipped, and Qdrant 404 responses are treated as
idempotent success. Failures remain recorded and can be retried. The same explicit
`--takeover` rule applies to interrupted writer reservations.

### Full content verification

```sh
qdrant-maintenance snapshots verify --run <run_id>
qdrant-maintenance snapshots verify --collection documents --node qdrant-0
```

Verification streams bytes through SHA-256 with bounded memory and checks the
expected size, checksum, and object metadata. Manifest-backed failures and
verification timestamps are persisted; freshness rejects recorded failures until
a successful verification clears them. Verification is separate from original
backup success and does not turn an incomplete run into a complete run.

The collection/node selector also checks unmanaged node-qualified snapshots using
their S3 SHA-256 metadata. Missing checksum metadata is an error; unmanaged results
are logged but have no manifest in which to persist health. Verification performs
no Qdrant requests. It reserves manifests while updating verification records;
`--takeover` is available only after the previous writer has terminated. It never
overrides an active pruning/expired state or repairs objects automatically.

## Dependency updates

[Renovate](https://docs.renovatebot.com/) is configured in
[`renovate.json`](renovate.json) to open dependency update PRs. To activate it,
install the [Renovate GitHub App](https://github.com/apps/renovate), grant it access
only to this repository, merge the configuration into the default branch, and
complete onboarding if prompted. The hosted app needs no additional workflow or
repository secret.

Routine update PRs are scheduled for Mondays between 06:00 and 11:00
(`Europe/Paris`), subject to the hosted app's execution times, with at most five
open concurrently. Security update PRs can bypass these limits. All PRs require
manual merging. The Dependency Dashboard issue lists pending updates and errors;
major Go module migrations require approval there before a PR is opened.

Coverage includes Go modules and checksums, Dockerfile and Compose images,
GitHub Actions, and the pinned `govulncheck` version in workflows. Go updates are
grouped across `go.mod`, the Docker build image, and the documented requirement.
AWS SDK, OpenTelemetry, and GitHub Actions minor/patch updates are grouped by
family; major updates remain separate. Docker images are pinned to digests by
Renovate PRs, allowing changes behind tags such as `latest` and `nonroot` to be
reviewed. Application image versions in deployment examples remain part of the
release process.

Existing PR checks validate tests, static analysis, reachable vulnerabilities,
and Docker builds on AMD64 and ARM64. Before merging Go updates, check that
`go.mod` and `go.sum` are consistent; before merging toolchain updates, check that
the minimum Go version and Docker image version agree. Compose services are not
integration-tested by these checks and need manual verification when updated.

Validate configuration changes using a Node.js version supported by the current
Renovate release:

```sh
npx --yes --package renovate -- renovate-config-validator --strict renovate.json
```

To pause updates, add `"enabled": false` to the configuration. Remove the app's
repository access in GitHub to disable it immediately.

## Release automation

Main-branch/PR checks run offline unit tests, race checks, static analysis, builds,
Docker builds and container smoke tests on native Linux AMD64 and ARM64 runners,
and a separate online `govulncheck` scan. Tagged stable versions (`vMAJOR.MINOR.PATCH`)
run the same checks before building static Linux AMD64 and ARM64 archives and a
multi-architecture image. The workflow publishes checksummed archives to GitHub
Releases and the image to `ghcr.io/<owner>/<repository>:<version>` using
`GITHUB_TOKEN`. Repository Actions/package permissions must permit publication.
The CLI's version event includes `git_commit`; release builds embed both fields.
Each release archive is extracted and its CLI help/version commands are tested on
the matching architecture before uploading it. Release assets are named
`qdrant-maintenance_<version>_linux_amd64.tar.gz` and
`qdrant-maintenance_<version>_linux_arm64.tar.gz`, with a `SHA256SUMS` file.

`scripts/build-release.sh v1.1.0 <git-sha> amd64 dist` prepares an archive locally
without publishing anything. Docker builds accept `VERSION` and `GIT_COMMIT`
build arguments. Creating/pushing a version tag triggers publication; ordinary
main-branch commits do not. No release tag or registry publication is created as
part of implementing these workflows.

### Storage permissions and concurrency

The backend must support S3 ETags and conditional `If-Match` / `If-None-Match`
writes. Grant `s3:ListBucket` on the bucket, `s3:GetObject` and `s3:PutObject` on
both snapshot objects and `runs/*`, and `s3:AbortMultipartUpload` on snapshot
objects. Pruning additionally requires `s3:DeleteObject` on snapshot objects;
manifest objects are retained. A role for `runs list` or `runs check` only needs
the corresponding read/list permissions. Verification writes manifest health and
therefore requires write access to `runs/*`.

Reservations coordinate commands acting on the same manifest. They are not a
bucket-wide lock: schedule creation and retention windows to avoid overlap, and
do not run different retention policies concurrently. Conditional writes detect
conflicts rather than silently overwriting another writer's state. No reservation
is automatically stolen based on elapsed time.

### Snapshot performance rollout

All nodes remain required. Each node processes its collections sequentially;
`--parallel-nodes` limits how many nodes can progress at once. The default is one.
An explicit flag overrides `QDRANT_PARALLEL_NODES`, including an invalid environment
value. Invalid selected values fail before backup work starts. Values above the
node count are accepted and capped to the available nodes. Retry uses the current
settings and preserves the recorded snapshot identities and manifest schema.

```sh
QDRANT_PARALLEL_NODES=2 qdrant-maintenance snapshots create --all
qdrant-maintenance snapshots create --parallel-nodes 3 documents
qdrant-maintenance runs retry --parallel-nodes 2 <run_id>
```

A single coordinator writes manifest checkpoints with the existing ETag conditions.
Workers wait for durable checkpoints before proceeding; source deletion requires a
verified upload and its successfully acknowledged checkpoint. A checkpoint failure
cancels outstanding work, stops scheduling and retains the writer reservation for
explicit recovery. Already-authorized remote operations may finish. Ordinary entry
failures do not stop other entries. Signal cancellation drains workers before final
state persistence. A final topology check is still required for completion.

The `snapshot.create.finished`, `snapshot.download.finished`,
`s3.upload.transfer.finished`, and `s3.upload.verify.finished` events distinguish
creation, download plus SHA-256/validation, S3 upload, and S3 metadata verification.
They include duration, size, outcome and backup identity. Transfer rates cover the
whole timed phase including retries. Failed S3 transfers report zero bytes because
the SDK does not expose partial committed bytes here. Existing events are retained;
the interval from `snapshot.created` to `snapshot.stored` still includes both
network transfers. Phase metrics use only operation/outcome dimensions. Run summaries
include effective node concurrency and configured multipart settings.

Before production activation:

1. Run at node concurrency `1`, multipart concurrency `2`, and parts `16` MiB.
   Record per-phase duration/rate, peak RSS, CPU/throttling, temporary disk usage,
   network utilization, and Qdrant request latency/error rate under comparable load.
2. Trial node concurrency `2`, then `3`, keeping multipart settings fixed. Stop
   increasing concurrency if resource limits or the application's latency/error
   budget are exceeded. Allow for simultaneous disk and network load on Qdrant.
3. If S3 upload remains limiting, compare multipart concurrency `2` and `4`, then
   parts `16`, `32`, and `64` MiB, changing one parameter at a time. The multipart
   concurrency pool is per upload: up to node concurrency × upload concurrency parts
   can be in flight. Measure RSS rather than assuming a fixed buffer allocation;
   uploads use seekable files. Provision disk for concurrent full temporary files.
4. The CronJob's 2 CPU / 1 GiB memory limit, 75 GiB ephemeral-storage request and
   100 GiB temporary-volume limit are example trial budgets for three ~20 GiB
   snapshots. Adapt to collection sizes, observed memory/CPU and node capacity;
   keep `concurrencyPolicy: Forbid`. Confirm the actual production deployment.
5. Check S3 region/endpoint and VPC route tables for the job's subnets; confirm
   whether the S3 gateway endpoint is used instead of NAT. This requires access to
   the deployment/infrastructure and cannot be established from this repository.
6. Verify a complete backup, exercise interruption plus `runs retry`, then restore
   onto an isolated compatible Qdrant cluster and validate collections/data. Keep
   production and test storage resources separate. Content verification alone is
   not a restore test. Approximately 20 minutes is an experimental target for the
   three-node case, not a guarantee; record actual results before acceptance.

Rollback: set `QDRANT_PARALLEL_NODES=1`, remove any overriding CLI flag, and restore
`S3_UPLOAD_CONCURRENCY=2` / `S3_UPLOAD_PART_SIZE_MIB=16`. Existing backup formats and
coverage rules are unchanged. Replica selection and direct streaming are outside
this change.
