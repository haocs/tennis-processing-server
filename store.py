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


class Store:
    def __init__(self, path, clock=time.time):
        self.path, self.clock = str(path), clock
        with self.tx() as db:
            db.executescript('''
            CREATE TABLE IF NOT EXISTS workers(id TEXT PRIMARY KEY, secret TEXT, body TEXT, seen REAL, sequence INTEGER DEFAULT 0, paused INTEGER DEFAULT 0);
            CREATE TABLE IF NOT EXISTS sources(id TEXT PRIMARY KEY, body TEXT, signature TEXT UNIQUE);
            CREATE TABLE IF NOT EXISTS tasks(id TEXT PRIMARY KEY, source_id TEXT, body TEXT, status TEXT, created REAL, version INTEGER DEFAULT 1, attempt TEXT, available REAL DEFAULT 0, result TEXT, publication TEXT DEFAULT 'not_requested');
            CREATE TABLE IF NOT EXISTS attempts(id TEXT PRIMARY KEY, task_id TEXT, worker_id TEXT, token TEXT, expires REAL, status TEXT, number INTEGER, body TEXT, result TEXT);
            CREATE TABLE IF NOT EXISTS events(attempt_id TEXT, sequence INTEGER, body TEXT, PRIMARY KEY(attempt_id,sequence));
            CREATE TABLE IF NOT EXISTS requests(scope TEXT,key TEXT,body TEXT,response TEXT,PRIMARY KEY(scope,key));
            CREATE TABLE IF NOT EXISTS watchers(id TEXT PRIMARY KEY,body TEXT,version INTEGER DEFAULT 1);
            CREATE TABLE IF NOT EXISTS observations(watcher_id TEXT,path TEXT,signature TEXT,stable REAL,PRIMARY KEY(watcher_id,path));
            CREATE TABLE IF NOT EXISTS scans(id TEXT PRIMARY KEY,body TEXT);
            CREATE TABLE IF NOT EXISTS publication_intents(task_id TEXT PRIMARY KEY,body TEXT,status TEXT);
            CREATE TABLE IF NOT EXISTS publications(source_hash TEXT PRIMARY KEY,attempt_id TEXT,version INTEGER DEFAULT 1);
            ''')

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
        token = secrets.token_urlsafe(32)
        identity = uid()
        public = {k: body[k] for k in ('display_name','platform','worker_version','capabilities','accessible_storage_ids')}
        if body.get('runtime'): public['runtime']=body['runtime']
        with self.tx() as db:
            db.execute('INSERT INTO workers(id,secret,body,seen) VALUES(?,?,?,?)', (identity, hashlib.sha256(token.encode()).hexdigest(), encode(public), self.clock()))
        return {'worker_id': identity, 'worker_token': token, 'heartbeat_seconds':15, 'lease_seconds':90, 'contract_version':1}

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
        db.execute('INSERT INTO tasks(id,source_id,body,status,created) VALUES(?,?,?,?,?)',(identity,body['source_id'],encode(body),'queued',self.clock()))
        return {'task_id':identity, 'status':'queued', 'version':1}

    def expire(self, db):
        rows=db.execute("SELECT * FROM attempts WHERE status IN ('leased','running') AND expires<=?",(self.clock(),)).fetchall()
        for row in rows:
            db.execute("UPDATE attempts SET status='lost' WHERE id=?",(row['id'],))
            task=db.execute('SELECT * FROM tasks WHERE id=?',(row['task_id'],)).fetchone()
            if task['status']=='cancel_requested':
                status='cancelled'
            else:
                status='retry_wait' if row['number'] < json.loads(task['body']).get('max_attempts',3) else 'failed'
            db.execute('UPDATE tasks SET status=?,available=?,version=version+1 WHERE id=?',(status,self.clock()+30,task['id']))
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
            rows=db.execute("SELECT * FROM tasks WHERE status='queued' ORDER BY json_extract(body,'$.priority') DESC,created,id").fetchall()
            for task in rows:
                spec=json.loads(task['body'])
                source=json.loads(db.execute('SELECT body FROM sources WHERE id=?',(task['source_id'],)).fetchone()['body'])
                if spec.get('runtime_requirements') and any(caps.get('runtime',{}).get(k)!=v for k,v in spec['runtime_requirements'].items()): continue
                if spec.get('executor','process-v2') not in caps['capabilities'] or source['storage_id'] not in caps['accessible_storage_ids']:
                    continue
                if caps.get('runtime') and not spec.get('runtime_requirements'):
                    spec['runtime_requirements']={k:v for k,v in caps['runtime'].items() if k in ('executor_version','build_digest','model_sha256')}
                    db.execute('UPDATE tasks SET body=? WHERE id=?',(encode(spec),task['id']))
                attempt=uid(); number=db.execute('SELECT COUNT(*) FROM attempts WHERE task_id=?',(task['id'],)).fetchone()[0]+1
                db.execute('INSERT INTO attempts VALUES(?,?,?,?,?,?,?,?,?)',(attempt,task['id'],worker,secrets.token_urlsafe(32),self.clock()+90,'leased',number,encode({'claimed_at':self.clock(),'request_id':body['request_id']}),None))
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
                db.execute('UPDATE attempts SET status=?,result=?,body=? WHERE id=?',(status,encode(public_result),encode(history),attempt))
                db.execute('UPDATE tasks SET status=?,result=?,publication=?,version=version+1,available=? WHERE id=?',(task_status,encode(public_result),publication,self.clock()+30,task['id']))
                return {'task_id':task['id'],'status':task_status,'publication_status':publication}
            return self.idempotent(db,'finish:'+attempt,key,body,action)

    def tasks(self):
        with self.tx() as db:
            self.expire(db)
            return [self.task_row(r) for r in db.execute('SELECT * FROM tasks ORDER BY created DESC')]

    def task_row(self, r):
        return dict(json.loads(r['body']),id=r['id'],status=r['status'],version=r['version'],created_at=r['created'],attempt_id=r['attempt'],result=json.loads(r['result']) if r['result'] else None,publication_status=r['publication'])

    def detail(self, identity):
        with self.tx() as db:
            row=db.execute('SELECT * FROM tasks WHERE id=?',(identity,)).fetchone()
            if not row: raise KeyError('task_not_found')
            result=self.task_row(row)
            result['attempts']=[{'id':r['id'],'worker_id':r['worker_id'],'number':r['number'],'status':r['status'],'server_timing':json.loads(r['body']),'lease_expires_at':r['expires'],'result':json.loads(r['result']) if r['result'] else None,'events':[json.loads(e['body']) for e in db.execute('SELECT body FROM events WHERE attempt_id=? ORDER BY sequence',(r['id'],))]} for r in db.execute('SELECT * FROM attempts WHERE task_id=? ORDER BY number',(identity,))]
            return result

    def workers(self):
        with self.tx() as db:
            return [dict(json.loads(r['body']),id=r['id'],last_seen=r['seen'],online=self.clock()-r['seen']<90,paused=bool(r['paused']),version=r['sequence']) for r in db.execute('SELECT * FROM workers ORDER BY seen DESC')]
