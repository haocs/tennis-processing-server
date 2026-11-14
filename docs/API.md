# Processing API v1

Base `/api/v1`, JSON only, max request 256 KiB. Large artifacts are NAS references
`{storage_id,path,sha256?,bytes?}`. Paths are relative, with no traversal/symlink
escape. Worker credentials use `Authorization: Bearer …`; attempts also require
`X-Worker-ID` and their current lease token. Registration alone uses the bootstrap
token from private server config. Never mirror credentials.

| Method and route | Request / behavior |
| --- | --- |
| POST `/workers/register` | display_name, platform, worker_version, capabilities[], accessible_storage_ids[], optional sanitized `ai_providers[]`, `ai_models[]`, `default_ai_model`. Returns individual worker ID/token (or refreshes existing authenticated worker). |
| POST `/workers/{id}/profile` | Authenticated worker capability/runtime/AI profile refresh (`capabilities`, `accessible_storage_ids`, `worker_version`, `runtime`, `ai_providers`, `ai_models`, `default_ai_model`) preserving worker ID and lease state. |
| POST `/workers/{id}/heartbeat` | increasing sequence; optional active `{attempt_id,lease_token}`, stage, last_progress_at, compact local_state, optional capability/runtime/AI refresh. Returns server_time/lease_expires_at/cancel/pause_new_tasks. |
| POST `/workers/{id}/claims` | request_id. 200 assignment or 204 idle. Busy worker gets its current assignment. Enforces derived `required_capabilities` (`cut-constraints-v1`, `hybrid-ai-vision-v1`), `required_storage_ids` (`processing`), and strict exact-model pinning when `allow_model_fallback=false`. |
| GET `/attempts/{id}` | Assigned worker only; lease status and acknowledged event sequence. |
| POST `/attempts/{id}/events` | lease_token, events[] with contiguous sequence, type, stage and compact metadata. Replays must match exactly. |
| POST `/attempts/{id}/complete` | Idempotency-Key; task_id, source_sha256, lease_token, final_event_sequence, timing, result_ref or dryrun findings. Independently validates `heuristic-diagnostics.json` (`<=512 KiB`). |
| POST `/attempts/{id}/fail` | Idempotency-Key; task_id, lease_token, final_event_sequence, error_code, retryable, timing. |
| GET `/attempts/{id}/heuristics` | Verified `heuristic-diagnostics.json` summary for an attempt. |
| GET/POST/DELETE `/attempts/{id}/pins` | Per-holder retention pins (`holder_kind`, `holder_id`, `ttl_seconds?`). Returns confirmed `media_state` and `pin_summary`; rejects `quarantined`/`evicted` attempts with 409. |
| GET `/sources` | Discovered/registered original file references. |
| POST `/sources` | storage_id=originals, relative path, optional known sha256; stat file generation. |
| GET `/sources/{id}/versions` | Verified output-version catalog for a 32-hex server `source_id`. |
| GET `/videos/{source_sha256}/versions` | Verified output-version catalog for a 64-hex `source_sha256` (`id`, `manifest`, `media_state`, `publication_status`, `playable`, `pin_summary`). |
| GET/POST `/feedback-snapshots` | Immutable cut-feedback snapshot (`<=128 KiB` inbound, rejects malformed/contradictory with 422, embeds verified `base_manifest` `<=256 KiB` at `processing/snapshots/<source_sha256>/<sha256>.json`, pins `base_attempt_id`). |
| GET `/feedback-snapshots/{id}` | Snapshot metadata and payload by `snapshot_id` (`fbs_…`) or `snapshot_sha256`. |
| POST `/tasks` | Idempotency-Key; source_id, type, parameters (`processing_strategy`: `vision_only` / `hybrid_ai_vision` / reserved `ai_only` -> `unsupported_processing_strategy`; per-task `ai_model`, `allow_model_fallback`, `ai_tuning.{aggressiveness,accuracy_tier}`), optional `feedback_snapshot_id`, priority?, max_attempts?, publication_policy=never/if_no_active_result. |
| GET `/tasks` | Filters status/source_id/type; cursor offset, limit <=100. |
| GET `/tasks/{id}` | Specification, all attempts/events/results, `media_state`, publication state. |
| POST `/tasks/{id}/rerun` | Idempotency-Key; parameter/type overrides or `feedback_snapshot_id`. Precedence `snapshot > caller > inherited` (409 `parameter_override_mismatch` on explicit caller conflict), derives `cut-constraints-v1` / `hybrid-ai-vision-v1`, forces `publication_policy=never`. |
| POST `/tasks/{id}/cancel` | expected_version. Queued cancellation or cooperative cancel request. |
| POST `/tasks/{id}/stage-candidate` | Explicit candidate staging to `library/media/<attempt>` and `library/metadata/candidates/<sha256>/<attempt>.json` without changing active serving. |
| GET `/tasks/{id}/activation-preview` | Current active attempt for compare-and-swap activation. |
| POST `/tasks/{id}/activate` | expected_active_attempt (null/legacy/attempt ID). Replay-safe activation from staging or `staged_candidate`; converts previous serving pin to `rollback_previous`. |
| POST `/retention/sweep` | Disabled-by-default candidate retention sweep and crash recovery for `quarantined` attempts. |
| GET `/workers` | Sanitized video workers (`items`, including advertised AI capability catalog), mirrored coaching/export workers (`external`), and aggregated `available_ai_models`. |
| POST `/external-sync` | Same-origin / `X-Tennis-Request` sanitized snapshot or delta sync (`domain`: `coaching`/`exports`, `workers[]`, `tasks[]`, `deleted[]`, `snapshot`, `complete`). Stores only sanitized status/error summaries (no prompts, answers, credentials, or host paths). |
| GET `/external-tasks` | Filtered/paginated mirrored external tasks (`kind`: `ai_coach`/`learning_plan`/`clip_interval_export`/`coaching_excerpt_render`, `status`, `domain`, `limit<=200`, `offset`) plus `workers[]`. |
| GET `/external-tasks/{domain}/{identity}` | Full sanitized metadata and error diagnostics for a single mirrored external task. |
| PATCH `/workers/{id}` | paused, expected_version; pause affects future assignments. |
| GET/POST `/watchers` | storage_id=originals, relative path, enabled (default false), settle_seconds, parameters (forbids `cut_constraints_ref`). |
| PATCH `/watchers/{id}` | expected_version, enabled/settle_seconds/parameters (forbids `cut_constraints_ref`). |
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

Version catalogs support `cursor`, `limit` (1–50), and `video_version_id`. Responses return `next_cursor` and are additionally bounded by encoded metadata size; clients follow pages rather than downloading unbounded run histories. Exact-version lookups avoid fetching unrelated full descriptors.

Cleanup starts its grace period when an eligible attempt is first marked, even if its TTL expired while the server was offline. A migration to these retention fields changes the attempts schema: back up stopped server state before deployment, and restore that matching backup when rolling back to an older container whose SQL predates these columns. This source change does not perform a deployment.
