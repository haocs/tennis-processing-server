import csv
import io
import json
import os
from pathlib import Path
import secrets
import time
from urllib.parse import urlsplit
from flask import Flask, jsonify, request, render_template, Response
from files import Files, atomic, inside
from store import Store, Conflict, encode, uid


def create_app(config=None):
    app=Flask(__name__)
    config=config or json.loads(Path(os.environ.get('PROCESSING_CONFIG','/config/server.json')).read_text())
    data=Path(config['data_dir']);data.mkdir(parents=True,exist_ok=True)
    store=Store(data/'processing.sqlite3');files=Files(config)
    app.config.update(MAX_CONTENT_LENGTH=256*1024,STORE=store,FILES=files,SETTINGS=config)

    @app.errorhandler(Conflict)
    def conflict(exc): return jsonify(error={'code':str(exc),'retryable':False}),409
    @app.errorhandler(ValueError)
    def invalid(exc): return jsonify(error={'code':str(exc),'retryable':False}),400
    @app.errorhandler(KeyError)
    def missing(exc): return jsonify(error={'code':str(exc).strip("'"),'retryable':False}),404
    @app.errorhandler(OSError)
    def unavailable(exc):
        app.logger.exception('Storage operation failed')
        return jsonify(error={'code':'storage_unavailable','retryable':True}),503

    def body():
        if not request.is_json: raise ValueError('json_required')
        value=request.get_json()
        if not isinstance(value,dict): raise ValueError('object_required')
        return value

    def worker(identity=None):
        identity=identity or request.headers.get('X-Worker-ID','')
        token=request.headers.get('Authorization','').removeprefix('Bearer ')
        if not store.authenticate(identity,token):
            from werkzeug.exceptions import Unauthorized
            raise Unauthorized()
        return identity

    @app.before_request
    def protect_portal():
        if request.method not in ('GET','HEAD','OPTIONS') and not request.path.startswith(('/api/v1/workers/','/api/v1/attempts/')) and request.path!='/api/v1/workers/register':
            origin=request.headers.get('Origin')
            if not request.is_json or request.headers.get('X-Tennis-Request')!='1' or (origin and urlsplit(origin).netloc!=request.host):
                return jsonify(error={'code':'same_origin_json_required','retryable':False}),403

    @app.get('/')
    def home(): return render_template('index.html')
    @app.get('/healthz')
    @app.get('/api/v1/healthz')
    def health(): return jsonify(status='ok')
    @app.get('/api/v1/readyz')
    def ready():
        store.tasks();status={k:False for k in config['storage']}
        for key in status:
            try: files.root(key);status[key]=True
            except OSError: pass
        return jsonify(database=True,storage=status),200 if all(status.values()) else 503

    @app.post('/api/v1/workers/register')
    def register():
        expected=config.get('registration_token','')
        if not expected or not secrets.compare_digest(request.headers.get('Authorization',''),'Bearer '+expected): return jsonify(error={'code':'unauthorized'}),401
        b=body()
        for key in ('display_name','platform','worker_version','capabilities','accessible_storage_ids'):
            if key not in b: raise ValueError('missing_'+key)
        if not isinstance(b['capabilities'],list) or not isinstance(b['accessible_storage_ids'],list): raise ValueError('invalid_capabilities')
        return jsonify(store.register(b)),201

    @app.post('/api/v1/workers/<identity>/heartbeat')
    def heartbeat(identity):
        worker(identity);b=body()
        if type(b.get('sequence')) is not int: raise ValueError('invalid_sequence')
        result=store.heartbeat(identity,b)
        # Mirror only sanitized local state; never lease/registration credentials.
        snapshot=next(w for w in store.workers() if w['id']==identity)
        try: atomic(files.root('processing')/'worker-state'/f'{identity}.json',snapshot)
        except OSError: app.logger.warning('Worker mirror unavailable')
        return jsonify(result)

    @app.post('/api/v1/workers/<identity>/claims')
    def claim(identity):
        worker(identity);b=body()
        if not isinstance(b.get('request_id'),str): raise ValueError('request_id_required')
        result=store.claim(identity,b)
        return jsonify(result) if result else ('',204)

    @app.get('/api/v1/attempts/<identity>')
    def attempt(identity):
        w=worker()
        with store.tx() as db:
            row=db.execute('SELECT * FROM attempts WHERE id=? AND worker_id=?',(identity,w)).fetchone()
            if not row: raise KeyError('attempt_not_found')
            last=db.execute('SELECT COALESCE(MAX(sequence),0) FROM events WHERE attempt_id=?',(identity,)).fetchone()[0]
            return jsonify(id=identity,status=row['status'],lease_expires_at=row['expires'],acknowledged_sequence=last)

    @app.post('/api/v1/attempts/<identity>/events')
    def events(identity): return jsonify(store.events(worker(),identity,body()))

    @app.post('/api/v1/attempts/<identity>/<action>')
    def finish(identity,action):
        if action not in ('complete','fail'): raise KeyError('route_not_found')
        b=body(); w=worker()
        result=store.finish(w,identity,b,action=='complete',request.headers.get('Idempotency-Key'),files.validate_result)
        return jsonify(result)

    def sources_data():
        with store.tx() as db: return db.execute('SELECT * FROM sources ORDER BY rowid DESC').fetchall()

    @app.get('/api/v1/sources')
    def sources(): return jsonify(items=[dict(json.loads(r['body']),id=r['id']) for r in sources_data()])
    @app.post('/api/v1/sources')
    def source():
        b=body();path=files.resolve(b);stat=path.stat()
        if path.suffix.lower() not in ('.mov','.mp4','.m4v'): raise ValueError('unsupported_source')
        b={'storage_id':b['storage_id'],'path':b['path'],'bytes':stat.st_size,'modified_ns':stat.st_mtime_ns,'sha256':b.get('sha256')}
        return jsonify(source_id=store.source(b)),201

    def task_spec(b):
        if b.get('type') not in ('process','dryrun','experiment'): raise ValueError('invalid_task_type')
        if b.get('executor','process-v2')!='process-v2': raise ValueError('unsupported_executor')
        if b.get('type')=='experiment' and not b.get('parameters'): raise ValueError('experiment_requires_parameters')
        if b.get('publication_policy','never') not in ('never','if_no_active_result'): raise ValueError('invalid_publication_policy')
        return dict(b,executor='process-v2',parameters=b.get('parameters',{}),priority=int(b.get('priority',0)),max_attempts=min(10,max(1,int(b.get('max_attempts',3)))),publication_policy=b.get('publication_policy','never'))

    @app.post('/api/v1/tasks')
    def tasks_create(): return jsonify(store.enqueue(task_spec(body()),request.headers.get('Idempotency-Key'))),201
    @app.get('/api/v1/tasks')
    def tasks():
        rows=store.tasks()
        for key in ('status','source_id','type'):
            if request.args.get(key): rows=[r for r in rows if r.get(key)==request.args[key]]
        offset=max(0,int(request.args.get('cursor',0))); limit=min(100,max(1,int(request.args.get('limit',50))))
        return jsonify(items=rows[offset:offset+limit],next_cursor=offset+limit if len(rows)>offset+limit else None)
    @app.get('/api/v1/tasks/<identity>')
    def detail(identity): return jsonify(store.detail(identity))
    @app.post('/api/v1/tasks/<identity>/rerun')
    def rerun(identity):
        previous=store.detail(identity);b=body()
        spec={k:previous[k] for k in ('source_id','type','executor','parameters','priority','max_attempts')}
        spec.update(b,parent_task_id=identity,publication_policy=b.get('publication_policy','never'))
        return jsonify(store.enqueue(task_spec(spec),request.headers.get('Idempotency-Key'))),201
    @app.post('/api/v1/tasks/<identity>/cancel')
    def cancel(identity):
        b=body()
        with store.tx() as db:
            row=db.execute('SELECT * FROM tasks WHERE id=?',(identity,)).fetchone()
            if not row: raise KeyError('task_not_found')
            if row['version']!=b['expected_version']: raise Conflict('version_conflict')
            status=row['status']
            if status in ('queued','retry_wait'): status='cancelled'
            elif status in ('leased','running'): status='cancel_requested'
            db.execute('UPDATE tasks SET status=?,version=version+1 WHERE id=?',(status,identity))
        return jsonify(status=status)
    @app.post('/api/v1/tasks/<identity>/activate')
    def activate(identity):
        b=body();return jsonify(files.activate(store,identity,b.get('expected_active_attempt')))
    @app.get('/api/v1/workers')
    def workers(): return jsonify(items=store.workers())
    @app.patch('/api/v1/workers/<identity>')
    def pause(identity):
        # This route is an admin action even though its prefix is worker-facing.
        b=body()
        if request.headers.get('X-Tennis-Request')!='1': return jsonify(error={'code':'admin_request_required'}),403
        with store.tx() as db:
            row=db.execute('SELECT * FROM workers WHERE id=?',(identity,)).fetchone()
            if not row: raise KeyError('worker_not_found')
            if b['expected_version']!=row['sequence']: raise Conflict('version_conflict')
            db.execute('UPDATE workers SET paused=?,sequence=sequence+1 WHERE id=?',(bool(b['paused']),identity))
        return jsonify(paused=bool(b['paused']))

    @app.get('/api/v1/metrics/timings')
    def metrics():
        items=[]
        for t in store.tasks():
            for a in store.detail(t['id'])['attempts']:
                if a['result']: items.append(dict(task_id=t['id'],attempt_id=a['id'],status=a['status'],**a['result'].get('timing',{})))
        if request.args.get('format')=='csv':
            out=io.StringIO();writer=csv.writer(out);writer.writerow(['task_id','attempt_id','status','total_seconds','stage','seconds'])
            for item in items:
                for stage in item.get('stages',[]): writer.writerow([item['task_id'],item['attempt_id'],item['status'],item.get('total_seconds'),stage['stage'],stage['seconds']])
            return Response(out.getvalue(),mimetype='text/csv')
        return jsonify(items=items)

    @app.post('/api/v1/import/legacy')
    def legacy():
        from legacy import import_library
        return jsonify(import_library(store,files))

    @app.get('/api/v1/tasks/<identity>/activation-preview')
    def activation_preview(identity):
        task=store.detail(identity)
        if not task['result'] or task['type']!='process': raise ValueError('no_process_result')
        digest=task['result']['source_sha256']
        with store.tx() as db:
            current=db.execute('SELECT attempt_id FROM publications WHERE source_hash=?',(digest,)).fetchone()
        active=current['attempt_id'] if current else ('legacy' if (files.root('library')/'metadata/sessions'/f'{digest}.json').exists() else None)
        return jsonify(expected_active_attempt=active,task_version=task['version'])

    from scheduler import routes
    routes(app,store,files,body)
    return app
