# Processing API v1

Base `/api/v1`, JSON only, max request 256 KiB. Large artifacts are NAS references
`{storage_id,path,sha256?,bytes?}`. Paths are relative, with no traversal/symlink
escape. Worker credentials use `Authorization: Bearer …`; attempts also require
`X-Worker-ID` and their current lease token. Registration alone uses the bootstrap
token from private server config. Never mirror credentials.

| Method and route | Request / behavior |
| --- | --- |
| POST `/workers/register` | display_name, platform, worker_version, capabilities[], accessible_storage_ids[]. Returns individual worker ID/token. |
| POST `/workers/{id}/heartbeat` | increasing sequence; optional active `{attempt_id,lease_token}`, stage, last_progress_at, compact local_state. Returns server_time/lease_expires_at/cancel/pause_new_tasks. |
| POST `/workers/{id}/claims` | request_id. 200 assignment or 204 idle. Busy worker gets its current assignment. |
| GET `/attempts/{id}` | Assigned worker only; lease status and acknowledged event sequence. |
| POST `/attempts/{id}/events` | lease_token, events[] with contiguous sequence, type, stage and compact metadata. Replays must match exactly. |
| POST `/attempts/{id}/complete` | Idempotency-Key; task_id, source_sha256, lease_token, final_event_sequence, timing, result_ref or dryrun findings. |
| POST `/attempts/{id}/fail` | Idempotency-Key; task_id, lease_token, final_event_sequence, error_code, retryable, timing. |
| GET `/sources` | Discovered/registered original file references. |
| POST `/sources` | storage_id=originals, relative path, optional known sha256; stat file generation. |
| POST `/tasks` | Idempotency-Key; source_id, type, parameters, priority?, max_attempts?, publication_policy=never/if_no_active_result. |
| GET `/tasks` | Filters status/source_id/type; cursor offset, limit <=100. |
| GET `/tasks/{id}` | Specification, all attempts/events/results, publication state. |
| POST `/tasks/{id}/rerun` | Idempotency-Key; parameter/type overrides. New task, default never auto-publish. |
| POST `/tasks/{id}/cancel` | expected_version. Queued cancellation or cooperative cancel request. |
| GET `/tasks/{id}/activation-preview` | Current active attempt for compare-and-swap activation. |
| POST `/tasks/{id}/activate` | expected_active_attempt (null/legacy/attempt ID). Process results only; idempotent for already active result. |
| GET `/workers` | Sanitized workers/snapshots; last seen and last progress separately. |
| PATCH `/workers/{id}` | paused, expected_version; pause affects future assignments. |
| GET/POST `/watchers` | storage_id=originals, relative path, enabled (default false), settle_seconds, parameters. |
| PATCH `/watchers/{id}` | expected_version, enabled/settle_seconds/parameters. |
| POST `/watchers/{id}/scan` | Immediate discovery scan, returns saved scan ID/status. |
| GET `/scans/{id}` | Persisted scan result. |
| POST `/import/legacy` | Import existing published results without decoding or changing media. |
| GET `/metrics/timings?format=json\|csv` | Retained completed/failed attempt/stage timings. |
| GET `/healthz`, `/readyz` | Liveness; storage/database readiness. |

Portal mutations require `Content-Type: application/json`, `X-Tennis-Request: 1`
and a matching Origin when supplied. Task and completion idempotency keys are
retained; conflicting reuse is 409. Workers must retain pending event/completion
payloads across network failures. Server receipt/expiry is authoritative; worker
uses a monotonic lease deadline based on the returned server time.

Error `{error:{code,retryable}}`; 400 invalid metadata/path, 401 worker identity,
404 missing record, 409 stale lease/version/sequence/idempotency/active-result,
413 metadata too large, 503 unavailable storage. No private raw exception paths.

Actual task stages: acquire_source, verify_source, prepare_proxy, analyze,
process, package_result, upload_result. `process` includes clip rendering,
joining and verification; finer per-clip recovery is a follow-up. Dryrun skips
upload_result and sends compact findings/timings, while local media remains.

Server lifecycle: queued -> leased -> running -> succeeded/failed/cancelled;
transient failure/lost attempt -> retry_wait -> queued. Server owns retries.
Publication states are separate: not_requested/pending/active/superseded/failed.
Task history is retained even when newer processing becomes active.
