"""Transactional task leases and append-only execution history."""
import contextlib
import hashlib
import json
import re
import secrets
import sqlite3
import time
import uuid
from feedback import AI_MODEL_RE, AI_PROVIDERS


def uid():
    return uuid.uuid4().hex


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


class Conflict(Exception):
    pass


PIN_HOLDER_KINDS = {
    'serving_active',
    'rollback_previous',
    'feedback_snapshot',
    'rerun_task',
    'web_review_lease',
    'comparison_session',
    'saved_comparison',
    'operator_manual',
    'operator_manual_pin',
}
EXTERNAL_DOMAINS = ('coaching', 'exports')
EXTERNAL_KINDS = (
    'ai_coach',
    'learning_plan',
    'clip_interval_export',
    'coaching_excerpt_render',
)
EXTERNAL_STATUSES = (
    'queued',
    'running',
    'succeeded',
    'failed',
    'cancelled',
)
MAX_EXTERNAL_ITEMS = 250


def _short_text(value, limit):
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError('invalid_external_text')
    text = ' '.join(value.split())
    if not text:
        return None
    return text[:limit]


def _opt_float(value, default=None):
    if value is None:
        return default
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError('invalid_external_timestamp')
    return float(value)


_WORKER_MODEL_NAME_RE = re.compile(r'^[A-Za-z0-9._-]{1,64}$')


def _validate_worker_ai_fields(public, body):
    """Validate and apply sanitized worker AI provider/model advertisement fields."""
    if not any(k in body for k in ('ai_providers', 'ai_models', 'default_ai_model')):
        return
    if 'ai_providers' in body:
        raw_providers = body['ai_providers']
        if raw_providers is None:
            providers = []
        elif isinstance(raw_providers, list) and len(raw_providers) <= 20:
            providers = []
            for p in raw_providers:
                if not isinstance(p, str) or p not in AI_PROVIDERS:
                    raise ValueError('invalid_ai_providers')
                if p not in providers:
                    providers.append(p)
        else:
            raise ValueError('invalid_ai_providers')
        public['ai_providers'] = providers
    if 'ai_models' in body:
        raw_models = body['ai_models']
        if raw_models is None:
            models = []
        elif isinstance(raw_models, list) and len(raw_models) <= 50:
            seen_ids = set()
            models = []
            allowed_providers = set(public.get('ai_providers') or []) if 'ai_providers' in body else None
            for item in raw_models:
                if not isinstance(item, dict):
                    raise ValueError('invalid_ai_models')
                provider = item.get('provider')
                model_name = item.get('model')
                ident = item.get('id')
                label = item.get('label')
                is_default = item.get('is_default', False)
                if (
                    not isinstance(provider, str)
                    or provider not in AI_PROVIDERS
                    or (allowed_providers is not None and provider not in allowed_providers)
                    or not isinstance(model_name, str)
                    or not _WORKER_MODEL_NAME_RE.fullmatch(model_name)
                    or not isinstance(ident, str)
                    or ident != f'{provider}:{model_name}'
                    or not AI_MODEL_RE.fullmatch(ident)
                    or ident in seen_ids
                    or not isinstance(label, str)
                    or '\r' in label
                    or '\n' in label
                    or not 1 <= len(label.strip()) <= 100
                    or type(is_default) is not bool
                ):
                    raise ValueError('invalid_ai_models')
                seen_ids.add(ident)
                models.append({
                    'id': ident,
                    'provider': provider,
                    'model': model_name,
                    'label': label.strip(),
                    'is_default': is_default,
                })
        else:
            raise ValueError('invalid_ai_models')
        public['ai_models'] = models
        if 'ai_providers' not in body:
            public['ai_providers'] = list(dict.fromkeys(m['provider'] for m in models))
    model_ids = {m['id'] for m in (public.get('ai_models') or []) if isinstance(m, dict) and 'id' in m}
    if 'default_ai_model' in body:
        default_model = body['default_ai_model']
        if default_model is not None:
            if not isinstance(default_model, str) or not AI_MODEL_RE.fullmatch(default_model) or default_model not in model_ids:
                raise ValueError('invalid_default_ai_model')
        public['default_ai_model'] = default_model
    elif 'ai_models' in body and public.get('default_ai_model') not in model_ids:
        public['default_ai_model'] = next((m['id'] for m in public.get('ai_models') or [] if m.get('is_default')), None)


def _sanitize_external_worker(raw, now):
    if not isinstance(raw, dict) or not isinstance(raw.get('id'), str) or not raw['id'].strip():
        raise ValueError('invalid_external_worker')
    worker_id = raw['id'].strip()[:120]
    seen = _opt_float(raw.get('last_seen', raw.get('seen')), now)
    ttl = _opt_float(raw.get('ttl_seconds'), 120.0)
    ttl = max(30.0, min(ttl, 3600.0))
    seq = raw.get('sequence', 0)
    if isinstance(seq, bool) or not isinstance(seq, int) or seq < 0:
        raise ValueError('invalid_external_worker')
    body = {
        'id': worker_id,
        'role': _short_text(raw.get('role'), 64) or 'worker',
        'label': _short_text(raw.get('label'), 120) or worker_id,
        'online_reported': bool(raw.get('online', True)),
        'ttl_seconds': ttl,
        'busy': bool(raw.get('busy', False)),
        'active_task': _short_text(raw.get('active_task'), 160),
        'active_kind': _short_text(raw.get('active_kind'), 64),
        'provider': _short_text(raw.get('provider'), 64),
        'model': _short_text(raw.get('model'), 120),
        'default_model': _short_text(raw.get('default_model'), 120),
        'requested_model': _short_text(raw.get('requested_model'), 120),
        'status_text': _short_text(raw.get('status_text'), 200),
        'error': _short_text(raw.get('error'), 300),
    }
    if 'providers' in raw and raw['providers'] is not None:
        body['providers'] = _validate_str_list(raw['providers'], 'invalid_external_worker')[:20]
    if 'capabilities' in raw and raw['capabilities'] is not None:
        body['capabilities'] = _validate_str_list(raw['capabilities'], 'invalid_external_worker')[:20]
    return worker_id, seen, int(bool(raw.get('paused', False))), seq, {k: v for k, v in body.items() if v is not None and v != ''}


def _sanitize_external_task(raw, domain, now):
    if not isinstance(raw, dict):
        raise ValueError('invalid_external_task')
    identity = _short_text(raw.get('identity'), 160)
    kind = raw.get('kind')
    status = raw.get('status')
    if not identity or kind not in EXTERNAL_KINDS or status not in EXTERNAL_STATUSES:
        raise ValueError('invalid_external_task')
    created = _opt_float(raw.get('created'), now)
    updated = _opt_float(raw.get('updated'), created)
    attempts = raw.get('attempts')
    if attempts is not None and (isinstance(attempts, bool) or not isinstance(attempts, int) or attempts < 0):
        raise ValueError('invalid_external_task')
    meta = raw.get('metadata')
    clean_meta = None
    if meta is not None:
        if not isinstance(meta, dict) or len(meta) > 20:
            raise ValueError('invalid_external_task')
        clean_meta = {}
        for k, v in meta.items():
            if isinstance(k, str) and len(k) <= 48 and isinstance(v, (str, int, float, bool)):
                clean_meta[k] = v[:200] if isinstance(v, str) else v
    body = {
        'domain': domain,
        'identity': identity,
        'kind': kind,
        'status': status,
        'raw_status': _short_text(raw.get('raw_status'), 40),
        'title': _short_text(raw.get('title'), 200),
        'summary': _short_text(raw.get('summary'), 320),
        'worker_id': _short_text(raw.get('worker_id'), 120),
        'worker_role': _short_text(raw.get('worker_role'), 64),
        'session_id': _short_text(raw.get('session_id'), 120),
        'session_name': _short_text(raw.get('session_name'), 160),
        'provider': _short_text(raw.get('provider'), 64),
        'model': _short_text(raw.get('model'), 120),
        'stage': _short_text(raw.get('stage'), 80),
        'attempts': attempts,
        'error': _short_text(raw.get('error'), 500),
        'error_code': _short_text(raw.get('error_code'), 80),
        'error_detail': _short_text(raw.get('error_detail'), 800),
        'started_at': _opt_float(raw.get('started_at')),
        'finished_at': _opt_float(raw.get('finished_at')),
        'created': created,
        'updated': updated,
        'metadata': clean_meta,
    }
    clean = {k: v for k, v in body.items() if v is not None and v != ''}
    return identity, kind, status, created, updated, clean


def _validate_str_list(items, error_code='invalid_capabilities'):
    if not isinstance(items, list) or any(not isinstance(x, str) or not x for x in items):
        raise ValueError(error_code)
    return list(dict.fromkeys(items))


class Store:
    def __init__(self, path, clock=time.time, retention=None):
        self.path, self.clock = str(path), clock
        self.retention = retention or {}
        import fcntl
        with open(self.path+'.schema.lock','a') as schema_lock:
            fcntl.flock(schema_lock,fcntl.LOCK_EX)
            with self.tx() as db:
                db.executescript('''
                CREATE TABLE IF NOT EXISTS workers(id TEXT PRIMARY KEY, secret TEXT, body TEXT, seen REAL, sequence INTEGER DEFAULT 0, paused INTEGER DEFAULT 0);
                CREATE TABLE IF NOT EXISTS sources(id TEXT PRIMARY KEY, body TEXT, signature TEXT UNIQUE);
                CREATE TABLE IF NOT EXISTS tasks(id TEXT PRIMARY KEY, source_id TEXT, body TEXT, status TEXT, created REAL, version INTEGER DEFAULT 1, attempt TEXT, available REAL DEFAULT 0, result TEXT, publication TEXT DEFAULT 'not_requested');
                CREATE TABLE IF NOT EXISTS attempts(id TEXT PRIMARY KEY, task_id TEXT, worker_id TEXT, token TEXT, expires REAL, status TEXT, number INTEGER, body TEXT, result TEXT, media_state TEXT DEFAULT 'unknown', gc_eligible_since REAL, media_evicted_at REAL);
                CREATE TABLE IF NOT EXISTS events(attempt_id TEXT, sequence INTEGER, body TEXT, PRIMARY KEY(attempt_id,sequence));
                CREATE TABLE IF NOT EXISTS requests(scope TEXT,key TEXT,body TEXT,response TEXT,PRIMARY KEY(scope,key));
                CREATE TABLE IF NOT EXISTS watchers(id TEXT PRIMARY KEY,body TEXT,version INTEGER DEFAULT 1);
                CREATE TABLE IF NOT EXISTS observations(watcher_id TEXT,path TEXT,signature TEXT,stable REAL,PRIMARY KEY(watcher_id,path));
                CREATE TABLE IF NOT EXISTS scans(id TEXT PRIMARY KEY,body TEXT);
                CREATE TABLE IF NOT EXISTS publication_intents(task_id TEXT PRIMARY KEY,body TEXT,status TEXT);
                CREATE TABLE IF NOT EXISTS staging_intents(task_id TEXT PRIMARY KEY,body TEXT,status TEXT);
                CREATE TABLE IF NOT EXISTS publications(source_hash TEXT PRIMARY KEY,attempt_id TEXT,version INTEGER DEFAULT 1);
                CREATE TABLE IF NOT EXISTS feedback_snapshots(id TEXT PRIMARY KEY,source_id TEXT NOT NULL,source_sha256 TEXT NOT NULL,base_task_id TEXT NOT NULL,base_attempt_id TEXT NOT NULL,base_video_version_id TEXT NOT NULL,snapshot_sha256 TEXT NOT NULL UNIQUE,storage_ref TEXT NOT NULL,body TEXT NOT NULL,created REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS artifact_pins(target_type TEXT NOT NULL,target_id TEXT NOT NULL,source_sha256 TEXT NOT NULL,holder_kind TEXT NOT NULL,holder_id TEXT NOT NULL,created REAL NOT NULL,expires REAL,metadata TEXT,PRIMARY KEY(target_type,target_id,holder_kind,holder_id));
                CREATE TABLE IF NOT EXISTS external_workers(domain TEXT NOT NULL,id TEXT NOT NULL,seen REAL NOT NULL,paused INTEGER NOT NULL DEFAULT 0,sequence INTEGER NOT NULL DEFAULT 0,body TEXT NOT NULL,updated REAL NOT NULL,PRIMARY KEY(domain,id));
                CREATE TABLE IF NOT EXISTS external_tasks(domain TEXT NOT NULL,identity TEXT NOT NULL,kind TEXT NOT NULL,status TEXT NOT NULL,worker_id TEXT,worker_role TEXT,session_id TEXT,session_name TEXT,provider TEXT,model TEXT,error TEXT,created REAL NOT NULL,updated REAL NOT NULL,body TEXT NOT NULL,PRIMARY KEY(domain,identity));
                CREATE INDEX IF NOT EXISTS idx_external_tasks_sort ON external_tasks(updated DESC,created DESC);
                ''')
                # executescript commits its input transaction; reacquire the write lock
                # before inspecting/altering a legacy schema during multi-process startup.
                db.execute('BEGIN IMMEDIATE')
                cols = {r['name'] for r in db.execute('PRAGMA table_info(attempts)').fetchall()}
                if 'media_state' not in cols:
                    db.execute("ALTER TABLE attempts ADD COLUMN media_state TEXT DEFAULT 'unknown'")
                if 'gc_eligible_since' not in cols:
                    db.execute('ALTER TABLE attempts ADD COLUMN gc_eligible_since REAL')
                if 'media_evicted_at' not in cols:
                    db.execute('ALTER TABLE attempts ADD COLUMN media_evicted_at REAL')
                # Legacy attempts migrate as 'unknown' (never blindly 'available'); only failed/cancelled/dryrun are 'never_rendered'.
                db.execute("UPDATE attempts SET media_state='never_rendered' WHERE media_state='unknown' AND (status IN ('failed','cancelled') OR task_id IN (SELECT id FROM tasks WHERE json_extract(body,'$.type')='dryrun'))")

    @property
    def retention_enabled(self):
        return bool((self.retention or {}).get('enabled', False))

    @contextlib.contextmanager
    def tx(self):
        db = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA journal_mode=WAL')
        db.execute('PRAGMA foreign_keys=ON')
        db.execute('BEGIN IMMEDIATE')
        try:
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    def register(self, body):
        capabilities = _validate_str_list(body.get('capabilities'), 'invalid_capabilities')
        accessible_storage_ids = _validate_str_list(body.get('accessible_storage_ids'), 'invalid_capabilities')
        for key in ('display_name', 'platform', 'worker_version'):
            if not isinstance(body.get(key), str) or not body[key]:
                raise ValueError('invalid_' + key)
        if body.get('runtime') is not None and not isinstance(body['runtime'], dict):
            raise ValueError('invalid_runtime')
        existing_id = body.get('worker_id')
        existing_token = body.get('worker_token')
        if existing_id and existing_token and self.authenticate(existing_id, existing_token):
            refreshed = self.refresh_worker_profile(existing_id, body)
            out = {'worker_id': existing_id, 'worker_token': existing_token, 'heartbeat_seconds': 15, 'lease_seconds': 90, 'contract_version': 1, 'capabilities': refreshed['capabilities'], 'accessible_storage_ids': refreshed['accessible_storage_ids']}
            for k in ('ai_providers', 'ai_models', 'default_ai_model'):
                if k in refreshed:
                    out[k] = refreshed[k]
            return out
        token = secrets.token_urlsafe(32)
        identity = uid()
        public = {
            'display_name': body['display_name'],
            'platform': body['platform'],
            'worker_version': body['worker_version'],
            'capabilities': capabilities,
            'accessible_storage_ids': accessible_storage_ids,
        }
        if body.get('runtime'):
            public['runtime'] = body['runtime']
        _validate_worker_ai_fields(public, body)
        with self.tx() as db:
            db.execute('INSERT INTO workers(id,secret,body,seen) VALUES(?,?,?,?)', (identity, hashlib.sha256(token.encode()).hexdigest(), encode(public), self.clock()))
        return {'worker_id': identity, 'worker_token': token, 'heartbeat_seconds': 15, 'lease_seconds': 90, 'contract_version': 1}

    def refresh_worker_profile(self, identity, body):
        with self.tx() as db:
            row = db.execute('SELECT * FROM workers WHERE id=?', (identity,)).fetchone()
            if not row:
                raise KeyError('worker_not_found')
            public = json.loads(row['body'])
            if 'capabilities' in body:
                public['capabilities'] = _validate_str_list(body['capabilities'], 'invalid_capabilities')
            if 'accessible_storage_ids' in body:
                public['accessible_storage_ids'] = _validate_str_list(body['accessible_storage_ids'], 'invalid_capabilities')
            for key in ('display_name', 'platform', 'worker_version'):
                if key in body:
                    if not isinstance(body[key], str) or not body[key]:
                        raise ValueError('invalid_' + key)
                    public[key] = body[key]
            if 'runtime' in body:
                if body['runtime'] is not None and not isinstance(body['runtime'], dict):
                    raise ValueError('invalid_runtime')
                public['runtime'] = body['runtime']
            _validate_worker_ai_fields(public, body)
            seq = max(row['sequence'] + 1, body['sequence']) if type(body.get('sequence')) is int else row['sequence'] + 1
            now = self.clock()
            db.execute('UPDATE workers SET body=?,seen=?,sequence=? WHERE id=?', (encode(public), now, seq, identity))
            return dict(public, id=identity, worker_id=identity, last_seen=now, online=True, paused=bool(row['paused']), version=seq, heartbeat_seconds=15, lease_seconds=90, contract_version=1)

    def authenticate(self, identity, token):
        with self.tx() as db:
            row = db.execute('SELECT secret FROM workers WHERE id=?', (identity,)).fetchone()
        return bool(row and secrets.compare_digest(row['secret'], hashlib.sha256(token.encode()).hexdigest()))

    def idempotent(self, db, scope, key, body, action):
        if not isinstance(key, str) or not 1 <= len(key) <= 128:
            raise ValueError('Idempotency-Key required')
        raw = encode(body)
        row = db.execute('SELECT * FROM requests WHERE scope=? AND key=?', (scope,key)).fetchone()
        if row:
            if row['body'] != raw:
                raise Conflict('idempotency_conflict')
            return json.loads(row['response'])
        result = action()
        db.execute('INSERT INTO requests VALUES(?,?,?,?)', (scope,key,raw,encode(result)))
        return result

    def source(self, body):
        signature = hashlib.sha256(encode({k:body.get(k) for k in ('storage_id','path','bytes','modified_ns')}).encode()).hexdigest()
        with self.tx() as db:
            row = db.execute('SELECT id,body FROM sources WHERE signature=?', (signature,)).fetchone()
            if row:
                if body.get('sha256') and not json.loads(row['body']).get('sha256'):
                    db.execute('UPDATE sources SET body=? WHERE id=?',(encode(body),row['id']))
                return row['id']
            identity=uid()
            db.execute('INSERT INTO sources VALUES(?,?,?)', (identity,encode(body),signature))
            return identity

    def enqueue(self, body, key):
        with self.tx() as db:
            if not db.execute('SELECT 1 FROM sources WHERE id=?',(body['source_id'],)).fetchone():
                raise KeyError('source_not_found')
            return self.idempotent(db,'tasks',key,body,lambda: self._enqueue(db,body))

    def _enqueue(self, db, body):
        identity=uid()
        now=self.clock()
        db.execute('INSERT INTO tasks(id,source_id,body,status,created) VALUES(?,?,?,?,?)',(identity,body['source_id'],encode(body),'queued',now))
        constraints_ref=(body.get('parameters') or {}).get('cut_constraints_ref')
        if isinstance(constraints_ref,dict) and constraints_ref.get('sha256'):
            snap=db.execute('SELECT * FROM feedback_snapshots WHERE snapshot_sha256=?',(constraints_ref['sha256'],)).fetchone()
            if snap:
                db.execute('INSERT OR REPLACE INTO artifact_pins VALUES(?,?,?,?,?,?,?,?)',('attempt',snap['base_attempt_id'],snap['source_sha256'],'rerun_task',identity,now,None,encode({'task_id':identity,'snapshot_id':snap['id']})))
                self._recompute_attempt_media_state(db,snap['base_attempt_id'])
        return {'task_id':identity, 'status':'queued', 'version':1}

    def release_task_pins(self, db, task_id):
        rows=db.execute("SELECT target_id FROM artifact_pins WHERE target_type='attempt' AND holder_kind='rerun_task' AND holder_id=?",(task_id,)).fetchall()
        if rows:
            now=self.clock()
            db.execute("DELETE FROM artifact_pins WHERE target_type='attempt' AND holder_kind='rerun_task' AND holder_id=?",(task_id,))
            for r in rows:
                att=db.execute('SELECT body FROM attempts WHERE id=?',(r['target_id'],)).fetchone()
                if att:
                    b=json.loads(att['body']) if att['body'] else {}
                    b['unpinned_at']=now
                    db.execute('UPDATE attempts SET body=? WHERE id=?',(encode(b),r['target_id']))
                self._recompute_attempt_media_state(db,r['target_id'])

    def expire_pins(self, db, retention_enabled=None):
        now=self.clock()
        expired=db.execute("SELECT DISTINCT target_id FROM artifact_pins WHERE target_type='attempt' AND expires IS NOT NULL AND expires<=?",(now,)).fetchall()
        if expired:
            db.execute("DELETE FROM artifact_pins WHERE target_type='attempt' AND expires IS NOT NULL AND expires<=?",(now,))
            for r in expired:
                att=db.execute('SELECT body FROM attempts WHERE id=?',(r['target_id'],)).fetchone()
                if att:
                    b=json.loads(att['body']) if att['body'] else {}
                    b['unpinned_at']=now
                    db.execute('UPDATE attempts SET body=? WHERE id=?',(encode(b),r['target_id']))
                self._recompute_attempt_media_state(db,r['target_id'],retention_enabled=retention_enabled)

    def pin_summary(self, db, attempt_id, gc_eligible_since=None):
        now=self.clock()
        rows=db.execute("SELECT * FROM artifact_pins WHERE target_type='attempt' AND target_id=? AND (expires IS NULL OR expires>?) ORDER BY created,holder_kind,holder_id",(attempt_id,now)).fetchall()
        pins=[{
            'holder_kind':r['holder_kind'],
            'holder_id':r['holder_id'],
            'created_at':r['created'],
            'expires_at':r['expires'],
            'metadata':json.loads(r['metadata']) if r['metadata'] else None,
        } for r in rows]
        return {
            'pinned':bool(pins),
            'active_pin_count':len(pins),
            'holder_kinds':sorted({r['holder_kind'] for r in rows}),
            'gc_eligible_since':gc_eligible_since,
            'pins':pins,
        }

    def _recompute_attempt_media_state(self, db, attempt_id, retention_enabled=None):
        retention_cfg=self.retention or {}
        if retention_enabled is None:
            retention_enabled=bool(retention_cfg.get('enabled',False))
        row=db.execute('SELECT * FROM attempts WHERE id=?',(attempt_id,)).fetchone()
        if not row:
            return None
        state=row['media_state']
        if state in ('quarantined','evicted','never_rendered','unknown'):
            return state
        active_pub=db.execute('SELECT 1 FROM publications WHERE attempt_id=?',(attempt_id,)).fetchone()
        if active_pub:
            db.execute("UPDATE attempts SET media_state='available',gc_eligible_since=NULL WHERE id=?",(attempt_id,))
            return 'available'
        now=self.clock()
        pin_count=db.execute("SELECT COUNT(*) FROM artifact_pins WHERE target_type='attempt' AND target_id=? AND (expires IS NULL OR expires>?)",(attempt_id,now)).fetchone()[0]
        task_row=db.execute('SELECT publication,created FROM tasks WHERE id=?',(row['task_id'],)).fetchone()
        was_published=bool(task_row and task_row['publication'] in ('active','superseded'))
        if pin_count>0:
            restored='available' if (state=='available' or was_published) else 'staged_candidate'
            db.execute('UPDATE attempts SET media_state=?,gc_eligible_since=NULL WHERE id=?',(restored,attempt_id))
            return restored
        if not retention_enabled:
            if state=='gc_eligible':
                restored='available' if was_published else 'staged_candidate'
                db.execute('UPDATE attempts SET media_state=?,gc_eligible_since=NULL WHERE id=?',(restored,attempt_id))
                return restored
            return state
        att_body=json.loads(row['body']) if row['body'] else {}
        unpinned_at=float(att_body.get('unpinned_at') or 0.0)
        if was_published:
            rollback_ttl=retention_cfg.get('rollback_ttl_seconds')
            if rollback_ttl is None:
                # Superseded serving remains permanent by default even if rollback pin is absent.
                if state!='available' or row['gc_eligible_since'] is not None:
                    db.execute("UPDATE attempts SET media_state='available',gc_eligible_since=NULL WHERE id=?",(attempt_id,))
                return 'available'
            base_ts=float(att_body.get('superseded_at') or att_body.get('finished_at') or (task_row['created'] if task_row else now) or now)
            eligible_at=max(base_ts+float(rollback_ttl),unpinned_at)
            if now>=eligible_at:
                since=row['gc_eligible_since'] if row['gc_eligible_since'] is not None else now
                db.execute("UPDATE attempts SET media_state='gc_eligible',gc_eligible_since=? WHERE id=?",(since,attempt_id))
                return 'gc_eligible'
            if state=='gc_eligible':
                db.execute("UPDATE attempts SET media_state='available',gc_eligible_since=NULL WHERE id=?",(attempt_id,))
                return 'available'
            return state
        cand_ttl=float(retention_cfg.get('candidate_ttl_seconds',14*86400))
        base_ts=float(att_body.get('staged_at') or att_body.get('finished_at') or (task_row['created'] if task_row else now) or now)
        eligible_at=max(base_ts+cand_ttl,unpinned_at)
        if state in ('staged_candidate','available','gc_eligible'):
            if now>=eligible_at:
                since=row['gc_eligible_since'] if row['gc_eligible_since'] is not None else now
                db.execute("UPDATE attempts SET media_state='gc_eligible',gc_eligible_since=? WHERE id=?",(since,attempt_id))
                return 'gc_eligible'
            if state=='gc_eligible':
                db.execute("UPDATE attempts SET media_state='staged_candidate',gc_eligible_since=NULL WHERE id=?",(attempt_id,))
                return 'staged_candidate'
        return state

    def pin_attempt(self, attempt_id, holder_kind, holder_id, ttl_seconds=None, metadata=None, retention_enabled=None):
        if holder_kind not in PIN_HOLDER_KINDS:
            raise ValueError('invalid_holder_kind')
        if not isinstance(holder_id,str) or not 1<=len(holder_id)<=128:
            raise ValueError('invalid_holder_id')
        if ttl_seconds is not None:
            import math
            if type(ttl_seconds) not in (int,float) or not math.isfinite(ttl_seconds) or ttl_seconds<=0 or ttl_seconds>365*86400:
                raise ValueError('invalid_ttl_seconds')
        with self.tx() as db:
            self.expire_pins(db,retention_enabled=retention_enabled)
            row=db.execute('SELECT * FROM attempts WHERE id=?',(attempt_id,)).fetchone()
            if not row:
                raise KeyError('attempt_not_found')
            if row['media_state'] in ('quarantined','evicted'):
                raise Conflict('media_already_evicted')
            if row['media_state'] not in ('available','staged_candidate','gc_eligible'):
                raise Conflict('media_not_available')
            result=json.loads(row['result']) if row['result'] else {}
            source_sha256=result.get('source_sha256') or ''
            now=self.clock()
            expires=(now+float(ttl_seconds)) if ttl_seconds is not None else (now+1800.0 if holder_kind in ('web_review_lease','comparison_session') else None)
            db.execute('INSERT OR REPLACE INTO artifact_pins VALUES(?,?,?,?,?,?,?,?)',('attempt',attempt_id,source_sha256,holder_kind,holder_id,now,expires,encode(metadata) if metadata is not None else None))
            self._recompute_attempt_media_state(db,attempt_id,retention_enabled=retention_enabled)
            updated=db.execute('SELECT * FROM attempts WHERE id=?',(attempt_id,)).fetchone()
            summary=self.pin_summary(db,attempt_id,updated['gc_eligible_since'])
            return {
                'attempt_id':attempt_id,
                'holder_kind':holder_kind,
                'holder_id':holder_id,
                'created_at':now,
                'expires_at':expires,
                'media_state':updated['media_state'],
                'pinned':summary['pinned'],
                'pin_summary':summary,
            }

    def unpin_attempt(self, attempt_id, holder_kind, holder_id, retention_enabled=None):
        if holder_kind not in PIN_HOLDER_KINDS:
            raise ValueError('invalid_holder_kind')
        if not isinstance(holder_id,str) or not 1<=len(holder_id)<=128:
            raise ValueError('invalid_holder_id')
        with self.tx() as db:
            row=db.execute('SELECT * FROM attempts WHERE id=?',(attempt_id,)).fetchone()
            if not row:
                raise KeyError('attempt_not_found')
            db.execute("DELETE FROM artifact_pins WHERE target_type='attempt' AND target_id=? AND holder_kind=? AND holder_id=?",(attempt_id,holder_kind,holder_id))
            att_body=json.loads(row['body']) if row['body'] else {}
            att_body['unpinned_at']=self.clock()
            db.execute('UPDATE attempts SET body=? WHERE id=?',(encode(att_body),attempt_id))
            self.expire_pins(db,retention_enabled=retention_enabled)
            self._recompute_attempt_media_state(db,attempt_id,retention_enabled=retention_enabled)
            updated=db.execute('SELECT * FROM attempts WHERE id=?',(attempt_id,)).fetchone()
            summary=self.pin_summary(db,attempt_id,updated['gc_eligible_since'])
            return {
                'attempt_id':attempt_id,
                'holder_kind':holder_kind,
                'holder_id':holder_id,
                'media_state':updated['media_state'],
                'pinned':summary['pinned'],
                'pin_summary':summary,
            }

    def get_attempt_pins(self, attempt_id, retention_enabled=None):
        with self.tx() as db:
            row=db.execute('SELECT * FROM attempts WHERE id=?',(attempt_id,)).fetchone()
            if not row:
                raise KeyError('attempt_not_found')
            self.expire_pins(db,retention_enabled=retention_enabled)
            self._recompute_attempt_media_state(db,attempt_id,retention_enabled=retention_enabled)
            updated=db.execute('SELECT * FROM attempts WHERE id=?',(attempt_id,)).fetchone()
            summary=self.pin_summary(db,attempt_id,updated['gc_eligible_since'])
            return {
                'attempt_id':attempt_id,
                'media_state':updated['media_state'],
                'pinned':summary['pinned'],
                'pin_summary':summary,
                'items':summary['pins'],
            }

    def expire(self, db):
        self.expire_pins(db)
        rows=db.execute("SELECT * FROM attempts WHERE status IN ('leased','running') AND expires<=?",(self.clock(),)).fetchall()
        for row in rows:
            db.execute("UPDATE attempts SET status='lost',media_state='never_rendered' WHERE id=?",(row['id'],))
            task=db.execute('SELECT * FROM tasks WHERE id=?',(row['task_id'],)).fetchone()
            if task['status']=='cancel_requested':
                status='cancelled'
            else:
                status='retry_wait' if row['number'] < json.loads(task['body']).get('max_attempts',3) else 'failed'
            db.execute('UPDATE tasks SET status=?,available=?,version=version+1 WHERE id=?',(status,self.clock()+30,task['id']))
            if status in ('cancelled','failed'):
                self.release_task_pins(db,task['id'])
        db.execute("UPDATE tasks SET status='queued',version=version+1 WHERE status='retry_wait' AND available<=?",(self.clock(),))

    def lease(self, db, attempt, worker, token):
        row=db.execute('SELECT * FROM attempts WHERE id=?',(attempt,)).fetchone()
        if not row:
            raise KeyError('attempt_not_found')
        if row['worker_id'] != worker or not secrets.compare_digest(row['token'],token or ''):
            raise Conflict('stale_lease')
        if row['status'] not in ('leased','running') or row['expires'] <= self.clock():
            raise Conflict('stale_lease')
        return row

    def claim(self, worker, body):
        with self.tx() as db:
            self.expire(db)
            w=db.execute('SELECT * FROM workers WHERE id=?',(worker,)).fetchone()
            active=db.execute("SELECT * FROM attempts WHERE worker_id=? AND status IN ('leased','running')",(worker,)).fetchone()
            if active:
                return self.assignment(db,active)
            if w['paused']:
                return None
            caps=json.loads(w['body'])
            worker_caps=set(caps.get('capabilities') or [])
            worker_storage=set(caps.get('accessible_storage_ids') or [])
            rows=db.execute("SELECT * FROM tasks WHERE status='queued' ORDER BY json_extract(body,'$.priority') DESC,created,id").fetchall()
            for task in rows:
                spec=json.loads(task['body'])
                params=spec.get('parameters') or {}
                strategy=params.get('processing_strategy','vision_only')
                if strategy=='ai_only':
                    continue
                source=json.loads(db.execute('SELECT body FROM sources WHERE id=?',(task['source_id'],)).fetchone()['body'])
                if spec.get('runtime_requirements') and any(caps.get('runtime',{}).get(k)!=v for k,v in spec['runtime_requirements'].items()): continue
                req_caps=set(spec.get('required_capabilities') or [])
                req_caps.add(spec.get('executor','process-v2'))
                if params.get('cut_constraints_ref'):
                    req_caps.add('cut-constraints-v1')
                if strategy=='hybrid_ai_vision':
                    req_caps.add('hybrid-ai-vision-v1')
                req_storage=set(spec.get('required_storage_ids') or [])
                req_storage.add(source['storage_id'])
                if spec.get('type')!='dryrun' or params.get('cut_constraints_ref'):
                    req_storage.add('processing')
                if not req_caps.issubset(worker_caps) or not req_storage.issubset(worker_storage):
                    continue
                if strategy=='hybrid_ai_vision':
                    worker_model_ids={m['id'] for m in (caps.get('ai_models') or []) if isinstance(m,dict) and 'id' in m}
                    if not worker_model_ids:
                        continue
                    if params.get('allow_model_fallback',True) is False:
                        req_model=params.get('ai_model','auto')
                        if req_model!='auto' and req_model not in worker_model_ids:
                            continue
                if caps.get('runtime') and not spec.get('runtime_requirements'):
                    spec['runtime_requirements']={k:v for k,v in caps['runtime'].items() if k in ('executor_version','build_digest','model_sha256')}
                    db.execute('UPDATE tasks SET body=? WHERE id=?',(encode(spec),task['id']))
                attempt=uid(); number=db.execute('SELECT COUNT(*) FROM attempts WHERE task_id=?',(task['id'],)).fetchone()[0]+1
                initial_media_state='never_rendered' if spec.get('type')=='dryrun' else 'unknown'
                db.execute('INSERT INTO attempts(id,task_id,worker_id,token,expires,status,number,body,result,media_state) VALUES(?,?,?,?,?,?,?,?,?,?)',(attempt,task['id'],worker,secrets.token_urlsafe(32),self.clock()+90,'leased',number,encode({'claimed_at':self.clock(),'request_id':body['request_id']}),None,initial_media_state))
                db.execute("UPDATE tasks SET status='leased',attempt=?,version=version+1 WHERE id=?",(attempt,task['id']))
                return self.assignment(db,db.execute('SELECT * FROM attempts WHERE id=?',(attempt,)).fetchone())
            return None

    def assignment(self, db, attempt):
        task=db.execute('SELECT * FROM tasks WHERE id=?',(attempt['task_id'],)).fetchone()
        spec=json.loads(task['body'])
        source=json.loads(db.execute('SELECT body FROM sources WHERE id=?',(task['source_id'],)).fetchone()['body'])
        return {'contract_version':1,'task':dict(spec,id=task['id'],input=source),'attempt':{'id':attempt['id'],'number':attempt['number'],'lease_token':attempt['token'],'lease_expires_at':attempt['expires'],'output_prefix':None if spec['type']=='dryrun' else {'storage_id':'processing','path':f"runs/{task['id']}/{attempt['id']}"}}}

    def heartbeat(self, worker, body):
        with self.tx() as db:
            self.expire(db)
            w=db.execute('SELECT * FROM workers WHERE id=?',(worker,)).fetchone()
            public=json.loads(w['body'])
            renewal={}
            if body.get('active'):
                a=body['active']; row=self.lease(db,a['attempt_id'],worker,a['lease_token'])
                db.execute('UPDATE attempts SET expires=? WHERE id=?',(self.clock()+90,row['id']))
                task=db.execute('SELECT status FROM tasks WHERE id=?',(row['task_id'],)).fetchone()
                renewal={'lease_expires_at':self.clock()+90,'cancel':task['status']=='cancel_requested'}
            if body['sequence'] > w['sequence']:
                if 'capabilities' in body:
                    public['capabilities']=_validate_str_list(body['capabilities'],'invalid_capabilities')
                if 'accessible_storage_ids' in body:
                    public['accessible_storage_ids']=_validate_str_list(body['accessible_storage_ids'],'invalid_capabilities')
                for key in ('display_name','platform','worker_version'):
                    if key in body:
                        if not isinstance(body[key],str) or not body[key]:
                            raise ValueError('invalid_'+key)
                        public[key]=body[key]
                if body.get('runtime'): public['runtime']=body['runtime']
                _validate_worker_ai_fields(public, body)
                public['snapshot']={k:body[k] for k in ('stage','progress','last_progress_at') if k in body}
                public['snapshot']['local_state']={k:body.get('local_state',{}).get(k) for k in ('task_id','attempt_id','acknowledged_sequence')}
                db.execute('UPDATE workers SET body=?,seen=?,sequence=? WHERE id=?',(encode(public),self.clock(),body['sequence'],worker))
            return dict(renewal, server_time=self.clock(),pause_new_tasks=bool(w['paused']))

    def events(self, worker, attempt, body):
        with self.tx() as db:
            row=self.lease(db,attempt,worker,body['lease_token'])
            last=db.execute('SELECT COALESCE(MAX(sequence),0) FROM events WHERE attempt_id=?',(attempt,)).fetchone()[0]
            for event in body['events']:
                seq=event['sequence']; raw=encode(event)
                old=db.execute('SELECT body FROM events WHERE attempt_id=? AND sequence=?',(attempt,seq)).fetchone()
                if old:
                    if old['body'] != raw: raise Conflict('event_sequence_conflict')
                    continue
                if seq != last+1: raise Conflict('event_sequence_gap')
                db.execute('INSERT INTO events VALUES(?,?,?)',(attempt,seq,raw)); last=seq
            db.execute("UPDATE attempts SET status='running' WHERE id=?",(attempt,))
            db.execute("UPDATE tasks SET status=CASE WHEN status='cancel_requested' THEN status ELSE 'running' END,version=version+1 WHERE id=?",(row['task_id'],))
            return {'acknowledged_sequence':last}

    def finish(self, worker, attempt, body, success, key, validate=None):
        with self.tx() as db:
            def action():
                row=self.lease(db,attempt,worker,body['lease_token'])
                task=db.execute('SELECT * FROM tasks WHERE id=?',(row['task_id'],)).fetchone()
                count=db.execute('SELECT COALESCE(MAX(sequence),0) FROM events WHERE attempt_id=?',(attempt,)).fetchone()[0]
                if body['final_event_sequence'] != count: raise Conflict('events_not_acknowledged')
                spec=json.loads(task['body'])
                if body.get('task_id')!=task['id']: raise ValueError('task_identity_mismatch')
                if success and task['status']=='cancel_requested': raise Conflict('cancel_requested')
                if success:
                    source=json.loads(db.execute('SELECT body FROM sources WHERE id=?',(task['source_id'],)).fetchone()['body'])
                    digest=body.get('source_sha256','')
                    import re
                    if not re.fullmatch('[a-f0-9]{64}',digest): raise ValueError('invalid_source_digest')
                    if source.get('sha256') and digest!=source['sha256']: raise ValueError('source_hash_mismatch')
                    if validate:
                        result=validate(spec,body,attempt)
                        if result and result['source_sha256']!=digest: raise ValueError('source_hash_mismatch')
                    source['sha256']=digest
                    db.execute('UPDATE sources SET body=? WHERE id=?',(encode(source),task['source_id']))
                cancelled=not success and (body.get('error_code')=='cancelled' or task['status']=='cancel_requested')
                status='succeeded' if success else 'cancelled' if cancelled else 'failed'
                task_status=status
                if status=='failed' and body.get('retryable') and row['number']<spec.get('max_attempts',3): task_status='retry_wait'
                publication='pending' if success and spec['type']=='process' and spec.get('publication_policy')=='if_no_active_result' else 'not_requested'
                public_result={k:v for k,v in body.items() if k not in ('lease_token',)}
                history=json.loads(row['body']);history['finished_at']=self.clock()
                media_state='never_rendered' if (not success or spec['type']=='dryrun') else row['media_state']
                db.execute('UPDATE attempts SET status=?,result=?,body=?,media_state=? WHERE id=?',(status,encode(public_result),encode(history),media_state,attempt))
                db.execute('UPDATE tasks SET status=?,result=?,publication=?,version=version+1,available=? WHERE id=?',(task_status,encode(public_result),publication,self.clock()+30,task['id']))
                if task_status in ('succeeded','failed','cancelled'):
                    self.release_task_pins(db,task['id'])
                return {'task_id':task['id'],'status':task_status,'publication_status':publication}
            return self.idempotent(db,'finish:'+attempt,key,body,action)

    def tasks(self):
        with self.tx() as db:
            self.expire(db)
            return [self.task_row(r,db) for r in db.execute('SELECT * FROM tasks ORDER BY created DESC')]

    def task_row(self, r, db=None):
        item=dict(json.loads(r['body']),id=r['id'],status=r['status'],version=r['version'],created_at=r['created'],attempt_id=r['attempt'],result=json.loads(r['result']) if r['result'] else None,publication_status=r['publication'])
        if db is not None and r['attempt']:
            att=db.execute('SELECT media_state,gc_eligible_since FROM attempts WHERE id=?',(r['attempt'],)).fetchone()
            if att:
                item['media_state']=att['media_state']
                item['pin_summary']=self.pin_summary(db,r['attempt'],att['gc_eligible_since'])
        return item

    def detail(self, identity):
        with self.tx() as db:
            row=db.execute('SELECT * FROM tasks WHERE id=?',(identity,)).fetchone()
            if not row: raise KeyError('task_not_found')
            result=self.task_row(row,db)
            result['attempts']=[{'id':r['id'],'worker_id':r['worker_id'],'number':r['number'],'status':r['status'],'media_state':r['media_state'],'gc_eligible_since':r['gc_eligible_since'],'media_evicted_at':r['media_evicted_at'],'pin_summary':self.pin_summary(db,r['id'],r['gc_eligible_since']),'server_timing':json.loads(r['body']),'lease_expires_at':r['expires'],'result':json.loads(r['result']) if r['result'] else None,'events':[json.loads(e['body']) for e in db.execute('SELECT body FROM events WHERE attempt_id=? ORDER BY sequence',(r['id'],))]} for r in db.execute('SELECT * FROM attempts WHERE task_id=? ORDER BY number',(identity,))]
            return result

    def workers(self):
        with self.tx() as db:
            return [dict(json.loads(r['body']),id=r['id'],last_seen=r['seen'],online=self.clock()-r['seen']<90,paused=bool(r['paused']),version=r['sequence']) for r in db.execute('SELECT * FROM workers ORDER BY seen DESC')]

    def available_ai_models(self):
        """Aggregate deduplicated AI models advertised across registered video workers."""
        catalog = {}
        for w in self.workers():
            online = bool(w.get('online'))
            for item in (w.get('ai_models') or []):
                if not isinstance(item, dict) or not item.get('id'):
                    continue
                ident = item['id']
                entry = catalog.get(ident)
                if entry is None:
                    entry = {
                        'id': ident,
                        'provider': item.get('provider'),
                        'model': item.get('model'),
                        'label': item.get('label') or ident,
                        'worker_ids': [],
                        'online_worker_count': 0,
                    }
                    catalog[ident] = entry
                if w['id'] not in entry['worker_ids']:
                    entry['worker_ids'].append(w['id'])
                    if online:
                        entry['online_worker_count'] += 1
        return sorted(catalog.values(), key=lambda m: (str(m.get('provider') or ''), str(m.get('id') or '')))

    def sync_external(self, domain, workers=None, tasks=None, deleted=None, snapshot=False, complete=True):
        """Upsert or replace a sanitized external worker/task snapshot for one domain."""
        if domain not in EXTERNAL_DOMAINS:
            raise ValueError('invalid_external_domain')
        now = self.clock()
        if workers is not None and not isinstance(workers, list):
            raise ValueError('invalid_external_workers')
        if tasks is not None and not isinstance(tasks, list):
            raise ValueError('invalid_external_tasks')
        if deleted is not None and not isinstance(deleted, list):
            raise ValueError('invalid_external_deleted')
        if (workers and len(workers) > MAX_EXTERNAL_ITEMS) or (tasks and len(tasks) > MAX_EXTERNAL_ITEMS) or (deleted and len(deleted) > MAX_EXTERNAL_ITEMS):
            raise ValueError('external_batch_too_large')
        worker_rows = [_sanitize_external_worker(w, now) for w in (workers or [])]
        task_rows = [_sanitize_external_task(t, domain, now) for t in (tasks or [])]
        del_ids = []
        for item in (deleted or []):
            ident = _short_text(item, 160)
            if not ident:
                raise ValueError('invalid_external_deleted')
            del_ids.append(ident)
        with self.tx() as db:
            if workers is not None:
                if snapshot:
                    keep = [w[0] for w in worker_rows]
                    if keep:
                        ph = ','.join('?' for _ in keep)
                        db.execute(f'DELETE FROM external_workers WHERE domain=? AND id NOT IN ({ph})', [domain, *keep])
                    else:
                        db.execute('DELETE FROM external_workers WHERE domain=?', (domain,))
                for wid, seen, paused, seq, clean in worker_rows:
                    db.execute(
                        'INSERT INTO external_workers(domain,id,seen,paused,sequence,body,updated) VALUES(?,?,?,?,?,?,?) ON CONFLICT(domain,id) DO UPDATE SET seen=excluded.seen,paused=excluded.paused,sequence=excluded.sequence,body=excluded.body,updated=excluded.updated',
                        (domain, wid, seen, paused, seq, encode(clean), now),
                    )
            if tasks is not None:
                for ident, kind, status, created, updated, clean in task_rows:
                    db.execute(
                        'INSERT INTO external_tasks(domain,identity,kind,status,worker_id,worker_role,session_id,session_name,provider,model,error,created,updated,body) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(domain,identity) DO UPDATE SET kind=excluded.kind,status=excluded.status,worker_id=excluded.worker_id,worker_role=excluded.worker_role,session_id=excluded.session_id,session_name=excluded.session_name,provider=excluded.provider,model=excluded.model,error=excluded.error,created=excluded.created,updated=excluded.updated,body=excluded.body',
                        (domain, ident, kind, status, clean.get('worker_id'), clean.get('worker_role'), clean.get('session_id'), clean.get('session_name'), clean.get('provider'), clean.get('model'), clean.get('error'), created, updated, encode(clean)),
                    )
                if snapshot and complete:
                    keep_tasks = [t[0] for t in task_rows]
                    if keep_tasks:
                        ph = ','.join('?' for _ in keep_tasks)
                        db.execute(f'DELETE FROM external_tasks WHERE domain=? AND identity NOT IN ({ph})', [domain, *keep_tasks])
                    else:
                        db.execute('DELETE FROM external_tasks WHERE domain=?', (domain,))
            for ident in del_ids:
                db.execute('DELETE FROM external_tasks WHERE domain=? AND identity=?', (domain, ident))
        return {'domain': domain, 'workers': len(worker_rows) if workers is not None else 0, 'tasks': len(task_rows) if tasks is not None else 0, 'deleted': len(del_ids)}

    def external_workers(self):
        """Return mirrored workers with dynamic staleness evaluated on server clock."""
        now = self.clock()
        with self.tx() as db:
            rows = db.execute('SELECT * FROM external_workers ORDER BY domain, id').fetchall()
        items = []
        for r in rows:
            body = json.loads(r['body'])
            ttl = float(body.get('ttl_seconds') or 120.0)
            online = bool(body.get('online_reported', True)) and (now - r['seen']) < ttl and (now - r['updated']) < max(ttl, 300.0)
            items.append({**body, 'domain': r['domain'], 'id': r['id'], 'last_seen': r['seen'], 'updated': r['updated'], 'online': online, 'paused': bool(r['paused']), 'sequence': r['sequence']})
        return items

    def external_tasks(self, limit=50, offset=0, status=None, kind=None, domain=None):
        """Return paginated mirrored tasks filtered by status, kind, or domain."""
        limit = max(1, min(int(limit), 200))
        offset = max(0, int(offset))
        clauses, params = [], []
        if status:
            if status not in EXTERNAL_STATUSES:
                raise ValueError('invalid_external_status')
            clauses.append('status=?')
            params.append(status)
        if kind:
            if kind not in EXTERNAL_KINDS:
                raise ValueError('invalid_external_kind')
            clauses.append('kind=?')
            params.append(kind)
        if domain:
            if domain not in EXTERNAL_DOMAINS:
                raise ValueError('invalid_external_domain')
            clauses.append('domain=?')
            params.append(domain)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ''
        with self.tx() as db:
            total = db.execute(f'SELECT COUNT(*) FROM external_tasks {where}', params).fetchone()[0]
            rows = db.execute(f'SELECT * FROM external_tasks {where} ORDER BY updated DESC, created DESC LIMIT ? OFFSET ?', [*params, limit, offset]).fetchall()
        items = [dict(json.loads(r['body']), domain=r['domain'], identity=r['identity'], kind=r['kind'], status=r['status'], created=r['created'], updated=r['updated']) for r in rows]
        return {'items': items, 'total': total, 'has_more': offset + limit < total}

    def external_task_detail(self, domain, identity):
        """Return full sanitized metadata for one mirrored external task."""
        with self.tx() as db:
            row = db.execute('SELECT * FROM external_tasks WHERE domain=? AND identity=?', (domain, identity)).fetchone()
        if not row:
            raise KeyError(identity)
        return dict(json.loads(row['body']), domain=row['domain'], identity=row['identity'], kind=row['kind'], status=row['status'], created=row['created'], updated=row['updated'])
