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
from feedback import InvalidFeedback
from store import Store, Conflict, encode, uid


def create_app(config=None):
    app=Flask(__name__)
    config=config or json.loads(Path(os.environ.get('PROCESSING_CONFIG','/config/server.json')).read_text())
    data=Path(config['data_dir']);data.mkdir(parents=True,exist_ok=True)
    store=Store(data/'processing.sqlite3',retention=config.get('retention'));files=Files(config)
    app.config.update(MAX_CONTENT_LENGTH=256*1024,STORE=store,FILES=files,SETTINGS=config)

    @app.errorhandler(InvalidFeedback)
    def invalid_feedback(exc): return jsonify(error={'code':str(exc),'retryable':False}),422
    @app.errorhandler(Conflict)
    def conflict(exc): return jsonify(error={'code':str(exc),'retryable':False}),409
    @app.errorhandler(ValueError)
    def invalid(exc):
        if isinstance(exc,InvalidFeedback):
            return jsonify(error={'code':str(exc),'retryable':False}),422
        return jsonify(error={'code':str(exc),'retryable':False}),400
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

    def _is_worker_mutation(path):
        if path=='/api/v1/workers/register':
            return True
        if path.startswith('/api/v1/workers/') and path.endswith(('/heartbeat','/claims','/profile','/capabilities')):
            return True
        if path.startswith('/api/v1/attempts/') and path.endswith(('/events','/complete','/fail')):
            return True
        return False

    @app.before_request
    def protect_portal():
        if request.method not in ('GET','HEAD','OPTIONS') and not _is_worker_mutation(request.path):
            origin=request.headers.get('Origin')
            needs_json=not (request.method=='DELETE' and '/pins/' in request.path and not request.data)
            if (needs_json and not request.is_json) or request.headers.get('X-Tennis-Request')!='1' or (origin and urlsplit(origin).netloc!=request.host):
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
        b=body()
        expected=config.get('registration_token','')
        auth_header=request.headers.get('Authorization','')
        bearer_token=auth_header.removeprefix('Bearer ') if auth_header.startswith('Bearer ') else ''
        existing_id=b.get('worker_id') or request.headers.get('X-Worker-ID','')
        existing_token=b.get('worker_token') or bearer_token
        is_bootstrap=bool(expected and secrets.compare_digest(auth_header,'Bearer '+expected))
        is_existing_worker=bool(existing_id and existing_token and store.authenticate(existing_id,existing_token))
        if not is_bootstrap and not is_existing_worker:
            return jsonify(error={'code':'unauthorized'}),401
        for key in ('display_name','platform','worker_version','capabilities','accessible_storage_ids'):
            if key not in b: raise ValueError('missing_'+key)
        if not isinstance(b['capabilities'],list) or not isinstance(b['accessible_storage_ids'],list): raise ValueError('invalid_capabilities')
        if is_existing_worker:
            b=dict(b,worker_id=existing_id,worker_token=existing_token)
        result=store.register(b)
        snapshot=next((w for w in store.workers() if w['id']==result['worker_id']),None)
        if snapshot:
            try: atomic(files.root('processing')/'worker-state'/f"{result['worker_id']}.json",snapshot)
            except OSError: app.logger.warning('Worker mirror unavailable')
        return jsonify(result),200 if is_existing_worker else 201

    @app.post('/api/v1/workers/<identity>/profile')
    @app.put('/api/v1/workers/<identity>/profile')
    @app.post('/api/v1/workers/<identity>/capabilities')
    def refresh_worker(identity):
        worker(identity);b=body()
        result=store.refresh_worker_profile(identity,b)
        snapshot=next((w for w in store.workers() if w['id']==identity),None)
        if snapshot:
            try: atomic(files.root('processing')/'worker-state'/f'{identity}.json',snapshot)
            except OSError: app.logger.warning('Worker mirror unavailable')
        return jsonify(result)

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

    @app.get('/api/v1/attempts/<identity>/heuristics')
    def attempt_heuristics(identity):
        return jsonify(files.get_attempt_heuristics(store,identity))

    @app.get('/api/v1/attempts/<identity>/pins')
    def get_pins(identity):
        return jsonify(store.get_attempt_pins(identity))

    @app.post('/api/v1/attempts/<identity>/pins')
    def create_pin(identity):
        b=body()
        return jsonify(store.pin_attempt(identity,b.get('holder_kind'),b.get('holder_id'),b.get('ttl_seconds'),b.get('metadata')))

    @app.delete('/api/v1/attempts/<identity>/pins')
    def delete_pin_body(identity):
        b=body()
        return jsonify(store.unpin_attempt(identity,b.get('holder_kind'),b.get('holder_id')))

    @app.delete('/api/v1/attempts/<identity>/pins/<holder_kind>/<holder_id>')
    def delete_pin_path(identity,holder_kind,holder_id):
        return jsonify(store.unpin_attempt(identity,holder_kind,holder_id))

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

    def version_page(items):
        try:offset=int(request.args.get('cursor','0'));limit=int(request.args.get('limit','50'))
        except ValueError:raise ValueError('invalid_catalog_page')
        if offset<0 or not 1<=limit<=50:raise ValueError('invalid_catalog_page')
        selected_version=request.args.get('video_version_id')
        if selected_version:items=[item for item in items if item['id']==selected_version]
        page=[];size=0
        for item in items[offset:offset+limit]:
            length=len(encode(item).encode())
            if length>384*1024:raise ValueError('version_descriptor_too_large')
            if page and size+length>384*1024:break
            page.append(item);size+=length
        end=offset+len(page)
        return jsonify(items=page,next_cursor=end if end<len(items) else None)

    @app.get('/api/v1/sources/<identity>/versions')
    def source_versions(identity):
        inc=request.args.get('include_non_playable')=='1'
        return version_page(files.list_video_versions(store,source_id=identity,include_non_playable=inc))

    @app.get('/api/v1/videos/<source_sha256>/versions')
    def video_versions(source_sha256):
        inc=request.args.get('include_non_playable')=='1'
        return version_page(files.list_video_versions(store,source_sha256=source_sha256,include_non_playable=inc))

    @app.post('/api/v1/feedback-snapshots')
    def create_snapshot():
        return jsonify(files.create_feedback_snapshot(store,body(),request.headers.get('Idempotency-Key'))),201

    @app.get('/api/v1/feedback-snapshots/<identity>')
    def get_snapshot(identity):
        return jsonify(files.get_feedback_snapshot(store,identity))

    @app.get('/api/v1/feedback-snapshots')
    def list_snapshots():
        with store.tx() as db:
            rows=db.execute('SELECT * FROM feedback_snapshots ORDER BY created DESC').fetchall()
        items=[]
        for r in rows:
            if request.args.get('source_id') and r['source_id']!=request.args['source_id']:
                continue
            if request.args.get('source_sha256') and r['source_sha256']!=request.args['source_sha256']:
                continue
            if request.args.get('base_attempt_id') and r['base_attempt_id']!=request.args['base_attempt_id']:
                continue
            sref=json.loads(r['storage_ref'])
            items.append({
                'id':r['id'],
                'snapshot_id':r['id'],
                'source_id':r['source_id'],
                'source_sha256':r['source_sha256'],
                'base_task_id':r['base_task_id'],
                'base_attempt_id':r['base_attempt_id'],
                'base_video_version_id':r['base_video_version_id'],
                'snapshot_sha256':r['snapshot_sha256'],
                'snapshot_ref':sref,
                'cut_constraints_ref':sref,
                'storage_ref':sref,
                'required_capabilities':['cut-constraints-v1'],
                'created_at':r['created'],
            })
        return jsonify(items=items)

    def task_spec(b, parent_task=None):
        return files.prepare_task_spec(store,b,parent_task=parent_task)

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
        spec={k:previous[k] for k in ('source_id','type','executor','parameters','priority','max_attempts') if k in previous}
        if 'parameters' in b and isinstance(b['parameters'],dict):
            merged_params=dict(previous.get('parameters') or {})
            if 'feedback_snapshot_id' in b and 'cut_constraints_ref' not in b['parameters']:
                merged_params.pop('cut_constraints_ref',None)
            merged_params.update(b['parameters'])
            b=dict(b,parameters=merged_params)
        elif 'feedback_snapshot_id' in b:
            merged_params=dict(previous.get('parameters') or {})
            merged_params.pop('cut_constraints_ref',None)
            b=dict(b,parameters=merged_params)
        spec.update(b,parent_task_id=identity,publication_policy=b.get('publication_policy','never'))
        return jsonify(store.enqueue(task_spec(spec,parent_task=previous),request.headers.get('Idempotency-Key'))),201
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
            if status=='cancelled':
                store.release_task_pins(db,identity)
        return jsonify(status=status)
    @app.post('/api/v1/tasks/<identity>/stage-candidate')
    def stage_candidate(identity):
        body()
        return jsonify(files.stage_candidate(store,identity))
    @app.post('/api/v1/tasks/<identity>/activate')
    def activate(identity):
        b=body();return jsonify(files.activate(store,identity,b.get('expected_active_attempt')))
    @app.post('/api/v1/retention/sweep')
    def retention_sweep():
        body()
        return jsonify(files.run_retention(store))
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
