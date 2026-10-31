import concurrent.futures
import hashlib
import json
import sys
from pathlib import Path
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from app import create_app




def worker(client):
    r=client.post('/api/v1/workers/register',json={'display_name':'Test','platform':'mac','worker_version':'2','capabilities':['process-v2'],'accessible_storage_ids':['originals','processing']},headers={'Authorization':'Bearer test-secret'})
    assert r.status_code==201,r.data
    data=r.json;return data['worker_id'],{'Authorization':'Bearer '+data['worker_token'],'X-Worker-ID':data['worker_id']}


def task(client,admin,source,kind='dryrun',key='task1'):
    r=client.post('/api/v1/tasks',json={'source_id':source,'type':kind,'parameters':{}},headers=dict(admin,**{'Idempotency-Key':key}));assert r.status_code==201,r.data
    return r.json['task_id']


def claim(client,w,h):
    r=client.post('/api/v1/workers/'+w+'/claims',json={'request_id':'claim1'},headers=h);assert r.status_code==200,r.data
    return r.json


def test_dryrun_complete_and_replay(env):
    app,c,h,s,cfg=env;w,wh=worker(c);t=task(c,h,s);a=claim(c,w,wh)['attempt']
    body={'lease_token':a['lease_token'],'events':[{'sequence':1,'type':'stage_completed','stage':'process'}]}
    for _ in range(2): assert c.post('/api/v1/attempts/'+a['id']+'/events',json=body,headers=wh).json['acknowledged_sequence']==1
    body={'task_id':t,'lease_token':a['lease_token'],'final_event_sequence':1,'source_sha256':hashlib.sha256(b'input').hexdigest(),'result_ref':None,'timing':{'total_seconds':5,'stages':[{'stage':'process','seconds':5}]}}
    for _ in range(2):
        r=c.post('/api/v1/attempts/'+a['id']+'/complete',json=body,headers=dict(wh,**{'Idempotency-Key':'complete1'}));assert r.status_code==200,r.data
    assert c.get('/api/v1/tasks/'+t).json['status']=='succeeded'
    assert not list(Path(cfg['storage']['processing']['path']).glob('runs/*'))
    assert 'process,5' in c.get('/api/v1/metrics/timings?format=csv').text


def test_atomic_claim_only_one_worker(env):
    app,c,h,s,cfg=env;task(c,h,s);workers=[worker(c) for _ in range(6)]
    def acquire(item):
        with app.test_client() as client:
            return client.post('/api/v1/workers/'+item[0]+'/claims',json={'request_id':'one'},headers=item[1]).status_code
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool: results=list(pool.map(acquire,workers))
    assert results.count(200)==1 and results.count(204)==5


def test_expired_lease_is_fenced_and_retry_is_new_attempt(env):
    app,c,h,s,cfg=env;store=app.config['STORE'];clock=[1000];store.clock=lambda:clock[0]
    w,wh=worker(c);t=task(c,h,s);a=claim(c,w,wh)['attempt'];clock[0]=1100
    r=c.post('/api/v1/attempts/'+a['id']+'/events',json={'lease_token':a['lease_token'],'events':[]},headers=wh);assert r.status_code==409
    c.get('/api/v1/tasks');clock[0]=1140
    b=claim(c,w,wh)['attempt'];assert b['id']!=a['id'] and b['number']==2


def test_idempotent_tasks_and_versioned_cancel(env):
    app,c,h,s,cfg=env;t=task(c,h,s);assert task(c,h,s)==t
    w,wh=worker(c);claim(c,w,wh)
    assert c.post('/api/v1/tasks/'+t+'/cancel',json={'expected_version':1},headers=h).status_code==409
    version=c.get('/api/v1/tasks/'+t).json['version']
    assert c.post('/api/v1/tasks/'+t+'/cancel',json={'expected_version':version},headers=h).json['status']=='cancel_requested'
    hb=c.post('/api/v1/workers/'+w+'/heartbeat',json={'sequence':1,'active':{'attempt_id':c.get('/api/v1/tasks/'+t).json['attempt_id'],'lease_token':claim(c,w,wh)['attempt']['lease_token']}},headers=wh)
    assert hb.json['cancel']


def test_paths_credentials_and_sequence_conflicts(env):
    app,c,h,s,cfg=env
    assert c.post('/api/v1/sources',json={'storage_id':'originals','path':'../bad.mov'},headers=h).status_code==400
    assert c.post('/api/v1/tasks',json={'type':'dryrun','source_id':s}).status_code==403
    w,wh=worker(c);task(c,h,s);a=claim(c,w,wh)['attempt']
    assert c.post('/api/v1/workers/'+w+'/claims',json={'request_id':'bad'}).status_code==401
    body={'lease_token':a['lease_token'],'events':[{'sequence':2,'type':'stage_started','stage':'process'}]}
    assert c.post('/api/v1/attempts/'+a['id']+'/events',json=body,headers=wh).status_code==409
    c.post('/api/v1/workers/'+w+'/heartbeat',json={'sequence':1,'local_state':{'lease_token':'secret','task_id':'test'}},headers=wh)
    mirror=Path(cfg['storage']['processing']['path'])/'worker-state'/f'{w}.json'
    assert 'lease_token' not in mirror.read_text()
    assert 'worker_token' not in c.get('/api/v1/workers').text


def test_watcher_stability_and_dedup(env):
    from scheduler import scan
    app,c,h,s,cfg=env;store=app.config['STORE'];clock=[1000];store.clock=lambda:clock[0]
    watcher=c.post('/api/v1/watchers',json={'storage_id':'originals','path':'','settle_seconds':10},headers=h).json
    for moment in (1000,1011,1022): clock[0]=moment;scan(store,app.config['FILES'],watcher['id'])
    assert len(store.tasks())==1


def test_one_upload_activation_rename_and_history(env):
    app,c,h,s,cfg=env;w,wh=worker(c);t=task(c,h,s,'process');a=claim(c,w,wh)['attempt']
    directory=Path(cfg['storage']['processing']['path'])/'runs'/t/a['id'];(directory/'media/Clips').mkdir(parents=True);(directory/'metadata').mkdir()
    names={'media/Practice.mp4':b'media','media/Clips/clip-001.mp4':b'clip','media/poster.jpg':b'poster','metadata/original-metadata.json':b'{}','metadata/Review.html':b'review'}
    records=[]
    for name,value in names.items():
        path=directory/name;path.write_bytes(value);records.append({'path':name,'bytes':len(value),'sha256':hashlib.sha256(value).hexdigest()})
    digest=hashlib.sha256(b'input').hexdigest()
    session={'id':digest,'source':{'sha256':digest},'video':{'file':'Practice.mp4'},'clips':[{'file':'Clips/clip-001.mp4'}],'artifacts':['Review.html','original-metadata.json']}
    (directory/'result.json').write_text(json.dumps({'attempt_id':a['id'],'task_id':t,'source_sha256':digest,'files':records,'session':session}))
    inode=(directory/'media/Practice.mp4').stat().st_ino
    payload={'task_id':t,'lease_token':a['lease_token'],'final_event_sequence':0,'source_sha256':digest,'result_ref':{'storage_id':'processing','path':f'runs/{t}/{a["id"]}/result.json'}}
    r=c.post('/api/v1/attempts/'+a['id']+'/complete',json=payload,headers=dict(wh,**{'Idempotency-Key':'done'}));assert r.status_code==200,r.data
    r=c.post('/api/v1/tasks/'+t+'/activate',json={'expected_active_attempt':None},headers=h);assert r.status_code==200,r.data
    lib=Path(cfg['storage']['library']['path']);assert (lib/'media'/a['id']/'Practice.mp4').stat().st_ino==inode
    assert not (directory/'media').exists()
    assert json.loads((lib/'metadata/sessions'/f'{digest}.json').read_text())['artifact_directory']==a['id']
    assert c.post('/api/v1/tasks/'+t+'/activate',json={'expected_active_attempt':None},headers=h).status_code==200
    assert c.post('/api/v1/tasks/'+t+'/rerun',json={},headers=dict(h,**{'Idempotency-Key':'rerun'})).json['task_id']!=t


def test_publication_recovers_crash_after_manifest_commit(env,monkeypatch):
    # Exercise the real single-upload activation fixture, injecting DB commit loss
    # immediately after the filesystem commit marker was written.
    app,c,h,s,cfg=env
    from files import atomic
    import files as module
    calls=[]
    def crash(path,value):
        atomic(path,value)
        if 'sessions' in str(path) and not calls:
            calls.append(True);raise OSError('simulated interruption after manifest write')
    monkeypatch.setattr(module,'atomic',crash)
    with pytest.raises(AssertionError):
        test_one_upload_activation_rename_and_history(env)
    monkeypatch.setattr(module,'atomic',atomic)
    store=app.config['STORE'];task=next(t for t in store.tasks() if t['status']=='succeeded')
    assert task['publication_status']=='pending'
    from scheduler import tick
    tick(store,app.config['FILES'])
    assert store.detail(task['id'])['publication_status']=='active'


def test_worker_credentials_never_appear_in_portal_history(env):
    app,c,h,s,cfg=env;w,wh=worker(c);t=task(c,h,s);a=claim(c,w,wh)['attempt']
    payload={'task_id':t,'lease_token':a['lease_token'],'final_event_sequence':0,'source_sha256':hashlib.sha256(b'input').hexdigest(),'result_ref':None}
    assert c.post('/api/v1/attempts/'+a['id']+'/complete',json=payload,headers=dict(wh,**{'Idempotency-Key':'finish'})).status_code==200
    for path in ('/api/v1/tasks','/api/v1/tasks/'+t,'/api/v1/workers','/api/v1/metrics/timings'):
        response=c.get(path);assert a['lease_token'] not in response.text
        assert wh['Authorization'].split(' ')[1] not in response.text


def test_legacy_import_does_not_enqueue_again_on_watcher_scan(env):
    from legacy import import_library
    from scheduler import scan
    app,c,h,s,cfg=env;store=app.config['STORE'];files=app.config['FILES']
    root=Path(cfg['storage']['library']['path'])/'metadata/sessions';root.mkdir(parents=True)
    digest=hashlib.sha256(b'input').hexdigest();(root/(digest+'.json')).write_text(json.dumps({'id':digest,'source':{'name':'test.mov'},'algorithm':'legacy'}))
    assert import_library(store,files)['imported']==1
    assert import_library(store,files)['imported']==0
    clock=[1000];store.clock=lambda:clock[0]
    watcher=c.post('/api/v1/watchers',json={'storage_id':'originals','path':'','settle_seconds':10},headers=h).json['id']
    scan(store,files,watcher);clock[0]=1011;scan(store,files,watcher)
    assert len(store.tasks())==1
    assert store.tasks()[0]['result']['timing']['total_seconds'] is None


def test_viewing_status_bridge_reports_worker_liveness_and_redacts_source_paths(env):
    from scheduler import mirror_viewing_status
    app,c,h,s,cfg=env;store=app.config['STORE'];files=app.config['FILES']
    mirror_viewing_status(store,files)
    status=Path(cfg['storage']['library']['path'])/'metadata/Processing-status.json'
    assert json.loads(status.read_text())['stage']=='Waiting for worker'
    w,wh=worker(c);identity=task(c,h,s,'process');a=claim(c,w,wh)['attempt']
    c.post('/api/v1/workers/'+w+'/heartbeat',json={'sequence':1,'stage':'process','last_progress_at':store.clock(),'local_state':{'task_id':identity},'active':{'attempt_id':a['id'],'lease_token':a['lease_token']}},headers=wh)
    mirror_viewing_status(store,files)
    value=json.loads(status.read_text())
    assert value['current_video']=='test.mov'
    assert value['files'][0]['status']=='processing'
    assert str(Path(cfg['storage']['originals']['path'])) not in status.read_text()
    assert a['lease_token'] not in status.read_text()
