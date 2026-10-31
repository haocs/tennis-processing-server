# Tennis Processing Server

LAN-hosted processing orchestration for [tennis-processing](https://github.com/haocs/tennis-processing).
One Docker application provides the API, persistent task/attempt history, worker
leases, an input scanner, publication control and a responsive control portal.
The [viewing website](https://github.com/haocs/tennis-web) remains separate.

## Storage and deployment

Pull the public versioned GHCR image; deploy application code as Docker, never a
source checkout on the NAS. Set the image digest and private storage paths in
`compose.yaml` environment variables; copy `server.example.json` outside Git.
Use a native NAS state directory, not SQLite over an SMB client mount.

`/tennis` is one bind mount of the published library root. Its hidden `.processing`
folder is the worker's staging root. With this layout, server activation is a
same-filesystem **rename**, not another video copy. The viewing app exposes only
allowlisted committed manifests/media and ignores hidden staging directories.
Only small manifests/state records are rewritten; NAS checksum verification reads
media, but does not rewrite it. Cross-filesystem publication is refused.

- Server: original files read-only; library/staging writable; native state writable.
- Worker: original mapping read-only, staging mapping writable, no library mapping.
  Use a dedicated NAS account/ACL for hard enforcement; merely omitting the mapping
  does not revoke a user's existing broad NAS permissions.
- Viewing site: library read-only, its own mutable preferences/state separately.

State volume contains task specs, events, results, worker tokens (hashes only) and
publication intents. Back up with SQLite's backup API, not a raw live file copy.
A consistent backup CLI is available in `manage.py`. No user data belongs in Git.

The application binds loopback by default. Serve it through a LAN-restricted NAS
reverse proxy. Registration requires a private bootstrap token; registered workers
receive individual credentials. The control portal has no separate user accounts
and must remain LAN-only. Mutations require same-origin JSON/custom header checks.
The image runs non-root, read-only, without added capabilities.

## Worker protocol

See [API contract](docs/API.md) and [architecture](docs/ARCHITECTURE.txt).
Heartbeat every 15 seconds, task lease 90 seconds; one task per worker initially.
Claims use transactions. Stale leases cannot report completion or activate results.
Events use strictly increasing replay-safe sequences. Completion/failure and task
creation use idempotency keys; reruns create new immutable task records.

Tasks: process, dryrun, experiment. Dryrun creates local media only; server history
and its NAS worker-state mirror still update. Experiment is an isolated run of the
existing executor with parameter overrides, not a training implementation.

Publication is separate from execution. First automatic results may activate only
when no existing result exists. Manual reruns default to never auto-activate.
Publication intents survive process crashes and replay the same directory moves.
Results retain their original manifest; after activation, media and metadata live
in the versioned library directories identified by the attempt ID. The server
records activation; use task/activation metadata to resolve moved artifacts.

Requires a viewing-site version supporting optional `artifact_directory` (v0.3.1+).
The v1 source identity remains stable, preserving video likes/court assignments.
Changed clip boundaries produce new clip IDs; no speculative like reconciliation.

## Migration and operation

1. Deploy server with watchers disabled; verify storage identity/permissions.
2. Stop the legacy Mac watcher after it finishes its queue.
3. `POST /api/v1/import/legacy` imports completed library records without rewriting
   media or inventing timing data. Unmatched original names are reported.
4. Register the Mac API worker and run one real dryrun, then one unpublished process.
5. Verify metadata, playback, timing, state recovery and activation/rollback.
6. Enable input watcher. Scanning does not automatically rerun imported sources.

The portal supports source selection, task creation/reruns, stage inspection,
cancellation, worker pause/resume, watcher enable/scan and CSV timing exports.
HTTP API enables explicit activation; activation portal UI is also available.

## Verification

```sh
python -m pip install -r requirements-dev.txt
python -m pytest -q
bash scripts/container-smoke.sh IMAGE
```

Core server tests require no AI/runtime. The real worker integration test also
requires a sibling `tennis-processing` checkout and its AV/imageio/numpy runtime;
it runs real encode/validation/upload with deterministic detection decisions.

P2: local worker dryrun-media cleanup task/retention. No automatic user-media deletion.
Windows adapter, per-clip resume, experiment training/evaluation and bounded
parallelism remain follow-ups. See the worker repository's `PIPELINE-V2-TODO.txt`.

First claim binds an unassigned task to the worker's code/model digests. Subsequent
attempts require the same build/model; heartbeat updates a worker's runtime
advertisement after upgrade. Rerun to select a new algorithm/build. Actual runtime
and effective cut settings remain in result metadata for reproducibility.

The scheduler mirrors a small compatibility status JSON/text into library metadata
for the existing viewing site's processing indicator. Worker offline/stale state
is separate from server liveness. Image publication also runs on version-tag pushes,
so releases can be performed with Git/API tooling without browser operations.
