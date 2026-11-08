"""Transactional task leases and append-only execution history."""
import contextlib
import hashlib
import json
import secrets
import sqlite3
import time
import uuid


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


def _validate_str_list(items, error_code='invalid_capabilities'):
    if not isinstance(items, list) or any(not isinstance(x, str) or not x for x in items):
        raise ValueError(error_code)
    return list(dict.fromkeys(items))


class Store:
    def __init__(self, path, clock=time.time, retention=None):
        self.path, self.clock = str(path), clock
        self.retention = retention or {}
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
            ''')
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
            return {'worker_id': existing_id, 'worker_token': existing_token, 'heartbeat_seconds': 15, 'lease_seconds': 90, 'contract_version': 1, 'capabilities': refreshed['capabilities'], 'accessible_storage_ids': refreshed['accessible_storage_ids']}
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
            db.execute("DELETE FROM artifact_pins WHERE target_type='attempt' AND holder_kind='rerun_task' AND holder_id=?",(task_id,))
            for r in rows:
                self._recompute_attempt_media_state(db,r['target_id'])

    def expire_pins(self, db, retention_enabled=None):
        now=self.clock()
        expired=db.execute("SELECT DISTINCT target_id FROM artifact_pins WHERE target_type='attempt' AND expires IS NOT NULL AND expires<=?",(now,)).fetchall()
        if expired:
            db.execute("DELETE FROM artifact_pins WHERE target_type='attempt' AND expires IS NOT NULL AND expires<=?",(now,))
            for r in expired:
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
        if retention_enabled is None:
            retention_enabled=self.retention_enabled
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
        task_row=db.execute('SELECT publication FROM tasks WHERE id=?',(row['task_id'],)).fetchone()
        was_published=bool(task_row and task_row['publication'] in ('active','superseded'))
        if pin_count>0:
            restored='available' if (state=='available' or was_published) else 'staged_candidate'
            db.execute('UPDATE attempts SET media_state=?,gc_eligible_since=NULL WHERE id=?',(restored,attempt_id))
            return restored
        if retention_enabled and state in ('staged_candidate','available'):
            since=row['gc_eligible_since'] if row['gc_eligible_since'] is not None else now
            db.execute("UPDATE attempts SET media_state='gc_eligible',gc_eligible_since=? WHERE id=?",(since,attempt_id))
            return 'gc_eligible'
        if not retention_enabled and state=='gc_eligible':
            restored='available' if was_published else 'staged_candidate'
            db.execute('UPDATE attempts SET media_state=?,gc_eligible_since=NULL WHERE id=?',(restored,attempt_id))
            return restored
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
                source=json.loads(db.execute('SELECT body FROM sources WHERE id=?',(task['source_id'],)).fetchone()['body'])
                if spec.get('runtime_requirements') and any(caps.get('runtime',{}).get(k)!=v for k,v in spec['runtime_requirements'].items()): continue
                req_caps=set(spec.get('required_capabilities') or [])
                req_caps.add(spec.get('executor','process-v2'))
                if (spec.get('parameters') or {}).get('cut_constraints_ref'):
                    req_caps.add('cut-constraints-v1')
                req_storage=set(spec.get('required_storage_ids') or [])
                req_storage.add(source['storage_id'])
                if spec.get('type')!='dryrun' or (spec.get('parameters') or {}).get('cut_constraints_ref'):
                    req_storage.add('processing')
                if not req_caps.issubset(worker_caps) or not req_storage.issubset(worker_storage):
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
