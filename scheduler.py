"""Single scanner/publisher process; bounded metadata scans, no video decoding."""
import json
import os
from pathlib import Path
import time
from flask import jsonify
from files import atomic, inside
from store import encode, uid, Conflict


def scan(store, files, watcher_id):
    scan_id=uid(); now=store.clock(); found=0
    with store.tx() as db:
        row=db.execute('SELECT body FROM watchers WHERE id=?',(watcher_id,)).fetchone()
        if not row: raise KeyError('watcher_not_found')
        watcher=json.loads(row['body'])
    try:
        root=files.root(watcher['storage_id'])
        folder=root if not watcher['path'] else inside(root,watcher['path'])
        for directory, dirs, names in os.walk(folder,followlinks=False):
            dirs[:]=[d for d in dirs if not d.startswith(('.','#','@')) and not (Path(directory)/d).is_symlink()]
            for name in sorted(names):
                path=Path(directory)/name
                if name.startswith('.') or path.is_symlink() or path.suffix.lower() not in ('.mov','.mp4','.m4v'): continue
                relative=path.relative_to(root).as_posix();stat=path.stat();sig=f'{stat.st_size}:{stat.st_mtime_ns}'
                with store.tx() as db:
                    previous=db.execute('SELECT * FROM observations WHERE watcher_id=? AND path=?',(watcher_id,relative)).fetchone()
                    if not previous or previous['signature']!=sig:
                        db.execute('INSERT OR REPLACE INTO observations VALUES(?,?,?,?)',(watcher_id,relative,sig,now));continue
                    if now-previous['stable']<watcher.get('settle_seconds',180): continue
                source=store.source({'storage_id':watcher['storage_id'],'path':relative,'bytes':stat.st_size,'modified_ns':stat.st_mtime_ns,'sha256':None})
                body={'source_id':source,'type':'process','executor':'process-v2','parameters':watcher.get('parameters',{}),'priority':0,'max_attempts':3,'publication_policy':'if_no_active_result','trigger':'watcher'}
                with store.tx() as db:
                    existing=db.execute("SELECT 1 FROM tasks WHERE source_id=? AND json_extract(body,'$.type')='process' LIMIT 1",(source,)).fetchone()
                if not existing: store.enqueue(body,f'watcher-{watcher_id}-{source}')
                found+=1
        result={'id':scan_id,'watcher_id':watcher_id,'status':'succeeded','observed_stable_files':found,'finished_at':store.clock()}
    except OSError:
        result={'id':scan_id,'watcher_id':watcher_id,'status':'failed','error_code':'storage_unavailable','finished_at':store.clock()}
    with store.tx() as db: db.execute('INSERT INTO scans VALUES(?,?)',(scan_id,encode(result)))
    return result


def tick(store,files):
    with store.tx() as db:
        store.expire(db)
        watchers=[r['id'] for r in db.execute("SELECT id FROM watchers WHERE json_extract(body,'$.enabled')=1")]
        publications=[dict(r) for r in db.execute("SELECT t.id,i.body AS intent FROM tasks t LEFT JOIN publication_intents i ON i.task_id=t.id WHERE t.publication='pending' AND t.status='succeeded'")]
    for identity in watchers: scan(store,files,identity)
    for publication in publications:
        identity=publication['id'];intent=json.loads(publication['intent']) if publication['intent'] else {}
        try: files.activate(store,identity,intent.get('expected_active'),policy=intent.get('policy','if_no_active_result'))
        except (OSError,ValueError,KeyError,Conflict):
            with store.tx() as db: db.execute("UPDATE tasks SET publication='failed' WHERE id=?",(identity,))


def routes(app,store,files,body):
    @app.get('/api/v1/watchers')
    def watchers():
        with store.tx() as db: return jsonify(items=[dict(json.loads(r['body']),id=r['id'],version=r['version']) for r in db.execute('SELECT * FROM watchers')])
    @app.post('/api/v1/watchers')
    def create_watcher():
        b=body();root=files.root(b['storage_id'])
        if b['storage_id']!='originals': raise ValueError('watcher_requires_originals')
        if b.get('path'): inside(root,b['path'])
        value={k:b[k] for k in ('storage_id','path','enabled','settle_seconds','parameters') if k in b}
        value.update(path=b.get('path',''),enabled=bool(b.get('enabled',False)),settle_seconds=max(10,int(b.get('settle_seconds',180))))
        identity=uid()
        with store.tx() as db: db.execute('INSERT INTO watchers VALUES(?,?,1)',(identity,encode(value)))
        return jsonify(id=identity,**value),201
    @app.patch('/api/v1/watchers/<identity>')
    def update_watcher(identity):
        b=body()
        with store.tx() as db:
            row=db.execute('SELECT * FROM watchers WHERE id=?',(identity,)).fetchone()
            if not row: raise KeyError('watcher_not_found')
            if b['expected_version']!=row['version']: raise Conflict('version_conflict')
            value=json.loads(row['body'])
            for key in ('enabled','parameters','settle_seconds'):
                if key in b: value[key]=b[key]
            value['settle_seconds']=max(10,int(value.get('settle_seconds',180)))
            db.execute('UPDATE watchers SET body=?,version=version+1 WHERE id=?',(encode(value),identity))
        return jsonify(id=identity,**value)
    @app.post('/api/v1/watchers/<identity>/scan')
    def run_scan(identity): return jsonify(scan(store,files,identity)),202
    @app.get('/api/v1/scans/<identity>')
    def scan_result(identity):
        with store.tx() as db:
            row=db.execute('SELECT body FROM scans WHERE id=?',(identity,)).fetchone()
            if not row: raise KeyError('scan_not_found')
            return jsonify(json.loads(row['body']))


if __name__=='__main__':
    import fcntl
    from app import create_app
    application=create_app(); store=application.config['STORE'];files=application.config['FILES']
    with open(str(store.path)+'.scheduler.lock','a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        while True:
            try: tick(store,files)
            except Exception: application.logger.exception('Scheduler tick failed')
            time.sleep(30)
