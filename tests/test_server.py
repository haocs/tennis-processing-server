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


def test_legacy_result_reference_is_immutable_after_active_manifest_changes(env):
    from legacy import import_library
    app,c,h,s,cfg=env;store=app.config['STORE'];files=app.config['FILES']
    root=Path(cfg['storage']['library']['path'])/'metadata/sessions';root.mkdir(parents=True)
    digest=hashlib.sha256(b'input').hexdigest();path=root/(digest+'.json')
    original={'id':digest,'source':{'name':'test.mov'},'algorithm':'legacy-v1'};path.write_text(json.dumps(original))
    import_library(store,files)
    reference=store.tasks()[0]['result']['result_ref']
    path.write_text(json.dumps(dict(original,algorithm='new-algorithm',processing={'attempt_id':'new'})))
    import_library(store,files)
    assert json.loads(files.resolve(reference).read_text())['algorithm']=='legacy-v1'
    assert store.tasks()[0]['result']['result_ref']==reference


def _write_process_attempt_bundle(cfg, task_id, attempt_id, digest, published_at=1700000000.0, clips=None, heuristics=None, extra_files=None):
    directory=Path(cfg['storage']['processing']['path'])/'runs'/task_id/attempt_id
    (directory/'metadata').mkdir(parents=True,exist_ok=True)
    if clips is None:
        clips=[{'id':'clip_1','file':'Clips/clip-001.mp4','original_start_seconds':1.0,'original_end_seconds':4.0,'duration_seconds':3.0}]
    names={'metadata/original-metadata.json':b'{"duration":10.0}'}
    artifacts=['original-metadata.json']
    if clips:
        (directory/'media/Clips').mkdir(parents=True,exist_ok=True)
        names['media/Practice.mp4']=b'practice-media-'+attempt_id.encode()
        names['media/poster.jpg']=b'poster'
        for c in clips:
            names['media/'+c['file']]=b'clip-bytes'
    if heuristics is not None:
        raw_h=heuristics if isinstance(heuristics,bytes) else json.dumps(heuristics).encode()
        names['metadata/heuristic-diagnostics.json']=raw_h
        artifacts.append('heuristic-diagnostics.json')
    for k,v in (extra_files or {}).items():
        names[k]=v
    records=[]
    for name,value in names.items():
        p=directory/name
        p.parent.mkdir(parents=True,exist_ok=True)
        p.write_bytes(value)
        records.append({'path':name,'bytes':len(value),'sha256':hashlib.sha256(value).hexdigest()})
    session={
        'id':digest,
        'published_at':published_at,
        'source':{'sha256':digest,'duration_seconds':10.0,'name':'test.mov'},
        'video':{'file':'Practice.mp4','duration_seconds':sum(c.get('duration_seconds',3.0) for c in clips) if clips else 0},
        'clips':clips,
        'artifacts':artifacts,
    }
    (directory/'result.json').write_text(json.dumps({
        'attempt_id':attempt_id,
        'task_id':task_id,
        'source_sha256':digest,
        'files':records,
        'session':session,
    }))
    return directory, session


def _valid_snapshot_payload(digest, attempt_id, published_at=1700000000.0):
    from feedback import version_id
    return {
        'schema_version':1,
        'source_sha256':digest,
        'base_attempt_id':attempt_id,
        'base_video_version_id':version_id(digest,published_at),
        'compiler_version':'cut-constraints-compiler-v1',
        'selected_evidence_revisions':[
            {
                'annotation_id':'ann_clip_1',
                'version':1,
                'created_at':1700000010.0,
                'updated_at':1700000010.0,
                'kind':'cut_feedback',
                'target':{'scope':'clip','clip_id':'clip_1','start_seconds':1.0,'end_seconds':4.0},
                'body':{'category':'trim_bounds'},
                'context':{'publication':published_at},
            },
            {
                'annotation_id':'ann_gap_1',
                'version':2,
                'created_at':1700000020.0,
                'updated_at':1700000025.0,
                'kind':'cut_feedback',
                'target':{'scope':'interval','start_seconds':5.0,'end_seconds':8.0},
                'body':{'category':'missed_rally'},
                'context':{'publication':published_at},
            },
        ],
        'confirmed_commands':{
            'keep_intervals_ms':[{'start_ms':5500,'end_ms':7500,'source_annotation_id':'ann_gap_1','source_revision':2}],
            'exclude_intervals_ms':[{'start_ms':8000,'end_ms':9500,'source_annotation_id':'ann_gap_1','source_revision':2}],
            'override_clip_bounds_ms':[{'clip_id':'clip_1','original_start_ms':1200,'original_end_ms':3800,'source_annotation_id':'ann_clip_1','source_revision':1}],
        },
        'parameter_overrides':{'padding_before':0.5,'target_region':[0.1,0.1,0.9,0.9]},
    }


def test_feedback_snapshot_validation_and_embedded_base_manifest(env):
    app,c,h,s,cfg=env
    w,wh=worker(c)
    t=task(c,h,s,'process','proc-base')
    a=claim(c,w,wh)['attempt']
    digest=hashlib.sha256(b'input').hexdigest()
    _write_process_attempt_bundle(cfg,t,a['id'],digest,published_at=1700000000.0)
    done={'task_id':t,'lease_token':a['lease_token'],'final_event_sequence':0,'source_sha256':digest,'result_ref':{'storage_id':'processing','path':f'runs/{t}/{a["id"]}/result.json'}}
    assert c.post('/api/v1/attempts/'+a['id']+'/complete',json=done,headers=dict(wh,**{'Idempotency-Key':'done-base'})).status_code==200
    assert c.post('/api/v1/tasks/'+t+'/activate',json={'expected_active_attempt':None},headers=h).status_code==200

    valid=_valid_snapshot_payload(digest,a['id'])

    # Client-forged base_manifest must be rejected 422
    forged=dict(valid,base_manifest={'id':digest})
    r=c.post('/api/v1/feedback-snapshots',json=forged,headers=dict(h,**{'Idempotency-Key':'snap-forged'}))
    assert r.status_code==422 and r.json['error']['code']=='client_base_manifest_forbidden'

    # Wrong source hash or passing 32-hex source_id as source_sha256 -> 422
    bad_src=dict(valid,source_sha256=s)
    assert c.post('/api/v1/feedback-snapshots',json=bad_src,headers=dict(h,**{'Idempotency-Key':'snap-badsrc'})).status_code==422

    # Wrong base_video_version_id -> 422
    bad_ver=dict(valid,base_video_version_id='v_'+'0'*32)
    assert c.post('/api/v1/feedback-snapshots',json=bad_ver,headers=dict(h,**{'Idempotency-Key':'snap-badver'})).status_code==422

    # Contradictory overlapping keep and exclude intervals -> 422
    contradictory=json.loads(json.dumps(valid))
    contradictory['confirmed_commands']['exclude_intervals_ms']=[{'start_ms':6000,'end_ms':7000,'source_annotation_id':'ann_gap_1','source_revision':2}]
    r=c.post('/api/v1/feedback-snapshots',json=contradictory,headers=dict(h,**{'Idempotency-Key':'snap-contra'}))
    assert r.status_code==422 and r.json['error']['code']=='contradictory_constraints'

    # Interval evidence overlapping a base kept clip [1.0, 4.0] -> 422
    bad_gap=json.loads(json.dumps(valid))
    bad_gap['selected_evidence_revisions'][1]['target']={'scope':'interval','start_seconds':2.0,'end_seconds':6.0}
    r=c.post('/api/v1/feedback-snapshots',json=bad_gap,headers=dict(h,**{'Idempotency-Key':'snap-badgap'}))
    assert r.status_code==422 and r.json['error']['code']=='evidence_not_in_removed_gap'

    # Boolean disguised as int in parameter_overrides -> 422
    bad_param=json.loads(json.dumps(valid))
    bad_param['parameter_overrides']={'padding_before':True}
    assert c.post('/api/v1/feedback-snapshots',json=bad_param,headers=dict(h,**{'Idempotency-Key':'snap-bool'})).status_code==422

    # Oversized inbound snapshot > 128 KiB -> 422
    huge=json.loads(json.dumps(valid))
    huge['selected_evidence_revisions'][0]['body']={'pad':'x'*(130*1024)}
    r=c.post('/api/v1/feedback-snapshots',json=huge,headers=dict(h,**{'Idempotency-Key':'snap-huge'}))
    assert r.status_code==422 and r.json['error']['code']=='feedback_snapshot_too_large'

    # Valid snapshot creation -> 201, embeds server-verified base_manifest on disk and pins base_attempt_id
    r=c.post('/api/v1/feedback-snapshots',json=valid,headers=dict(h,**{'Idempotency-Key':'snap-ok'}))
    assert r.status_code==201,r.data
    snap=r.json
    assert snap['snapshot_id'].startswith('fbs_')
    assert snap['base_task_id']==t
    assert snap['base_attempt_id']==a['id']
    assert snap['source_id']==s and snap['source_sha256']==digest and snap['source_id']!=snap['source_sha256']
    assert snap['required_capabilities']==['cut-constraints-v1']
    ref=snap['snapshot_ref']
    disk_path=Path(cfg['storage']['processing']['path'])/ref['path']
    raw=disk_path.read_bytes()
    assert len(raw)==ref['bytes'] and hashlib.sha256(raw).hexdigest()==ref['sha256']
    artifact=json.loads(raw.decode())
    assert artifact['base_manifest']['id']==digest
    assert artifact['base_manifest']['clips'][0]['id']=='clip_1'
    pins=c.get('/api/v1/attempts/'+a['id']+'/pins').json
    assert 'feedback_snapshot' in pins['pin_summary']['holder_kinds']


def test_strict_derived_capabilities_omitted_refs_and_worker_profile_refresh(env):
    app,c,h,s,cfg=env
    w,wh=worker(c)
    t=task(c,h,s,'process','base-t')
    a=claim(c,w,wh)['attempt']
    digest=hashlib.sha256(b'input').hexdigest()
    _write_process_attempt_bundle(cfg,t,a['id'],digest)
    done={'task_id':t,'lease_token':a['lease_token'],'final_event_sequence':0,'source_sha256':digest,'result_ref':{'storage_id':'processing','path':f'runs/{t}/{a["id"]}/result.json'}}
    assert c.post('/api/v1/attempts/'+a['id']+'/complete',json=done,headers=dict(wh,**{'Idempotency-Key':'done-1'})).status_code==200
    assert c.post('/api/v1/tasks/'+t+'/stage-candidate',json={},headers=h).status_code==200

    snap=c.post('/api/v1/feedback-snapshots',json=_valid_snapshot_payload(digest,a['id']),headers=dict(h,**{'Idempotency-Key':'snap-1'})).json

    # Conflicting cut_constraints_ref on rerun must fail 409
    bad_rerun=c.post('/api/v1/tasks/'+t+'/rerun',json={'feedback_snapshot_id':snap['snapshot_id'],'parameters':{'cut_constraints_ref':{'storage_id':'processing','path':'snapshots/other.json'}}},headers=dict(h,**{'Idempotency-Key':'rerun-conflict'}))
    assert bad_rerun.status_code==409

    # Constrained rerun via feedback_snapshot_id (and omitted required_capabilities) derives cut-constraints-v1 and forces publication_policy=never
    rerun_res=c.post('/api/v1/tasks/'+t+'/rerun',json={'feedback_snapshot_id':snap['snapshot_id']},headers=dict(h,**{'Idempotency-Key':'rerun-ok'}))
    assert rerun_res.status_code==201,rerun_res.data
    rt_id=rerun_res.json['task_id']
    rt_detail=c.get('/api/v1/tasks/'+rt_id).json
    assert rt_detail['required_capabilities']==['cut-constraints-v1']
    assert 'processing' in rt_detail['required_storage_ids']
    assert rt_detail['publication_policy']=='never'
    assert rt_detail['parameters']['cut_constraints_ref']['sha256']==snap['snapshot_sha256']
    assert rt_detail['parameters']['padding_before']==0.5

    # Also verify direct POST /api/v1/tasks with cut_constraints_ref and omitted required_capabilities derives cut-constraints-v1
    direct_res=c.post('/api/v1/tasks',json={'source_id':s,'type':'dryrun','parameters':{'cut_constraints_ref':snap['snapshot_ref']}},headers=dict(h,**{'Idempotency-Key':'direct-constrained'}))
    assert direct_res.status_code==201
    direct_detail=c.get('/api/v1/tasks/'+direct_res.json['task_id']).json
    assert direct_detail['required_capabilities']==['cut-constraints-v1']
    assert 'processing' in direct_detail['required_storage_ids']

    # Base attempt now holds both feedback_snapshot and rerun_task pins
    pins_before=c.get('/api/v1/attempts/'+a['id']+'/pins').json
    assert {'feedback_snapshot','rerun_task'}.issubset(set(pins_before['pin_summary']['holder_kinds']))

    # Un-upgraded worker (only 'process-v2') cannot claim constrained tasks!
    assert c.post('/api/v1/workers/'+w+'/claims',json={'request_id':'try-unupgraded'},headers=wh).status_code==204

    # Authenticated capability profile refresh upgrades worker in-place without losing worker_id
    prof=c.post('/api/v1/workers/'+w+'/profile',json={'worker_version':'2.1','capabilities':['process-v2','cut-constraints-v1'],'accessible_storage_ids':['originals','processing']},headers=wh)
    assert prof.status_code==200 and prof.json['worker_id']==w
    assert 'cut-constraints-v1' in prof.json['capabilities']

    # Upgraded worker now claims the constrained rerun task
    claimed=claim(c,w,wh)
    assert claimed['task']['id']==rt_id
    # Cancel the direct dryrun task and complete the rerun task; verify rerun_task pins release while feedback_snapshot pin remains
    assert c.post('/api/v1/tasks/'+direct_res.json['task_id']+'/cancel',json={'expected_version':1},headers=h).status_code==200
    _write_process_attempt_bundle(cfg,rt_id,claimed['attempt']['id'],digest,published_at=1700000100.0)
    done2={'task_id':rt_id,'lease_token':claimed['attempt']['lease_token'],'final_event_sequence':0,'source_sha256':digest,'result_ref':{'storage_id':'processing','path':f'runs/{rt_id}/{claimed["attempt"]["id"]}/result.json'}}
    assert c.post('/api/v1/attempts/'+claimed['attempt']['id']+'/complete',json=done2,headers=dict(wh,**{'Idempotency-Key':'done-2'})).status_code==200
    pins_after=c.get('/api/v1/attempts/'+a['id']+'/pins').json
    assert 'rerun_task' not in pins_after['pin_summary']['holder_kinds']
    assert 'feedback_snapshot' in pins_after['pin_summary']['holder_kinds']


def test_stage_candidate_activation_crash_replay_and_catalog_identities(env,monkeypatch):
    app,c,h,s,cfg=env
    w,wh=worker(c)
    digest=hashlib.sha256(b'input').hexdigest()

    # Run 1 -> activate as serving
    t1=task(c,h,s,'process','run-1')
    a1=claim(c,w,wh)['attempt']
    _write_process_attempt_bundle(cfg,t1,a1['id'],digest,published_at=1700000000.0)
    assert c.post('/api/v1/attempts/'+a1['id']+'/complete',json={'task_id':t1,'lease_token':a1['lease_token'],'final_event_sequence':0,'source_sha256':digest,'result_ref':{'storage_id':'processing','path':f'runs/{t1}/{a1["id"]}/result.json'}},headers=dict(wh,**{'Idempotency-Key':'c1'})).status_code==200
    assert c.post('/api/v1/tasks/'+t1+'/activate',json={'expected_active_attempt':None},headers=h).status_code==200

    # Run 2 -> with valid heuristic-diagnostics.json
    t2=task(c,h,s,'process','run-2')
    a2=claim(c,w,wh)['attempt']
    from feedback import version_id
    pub2=1700000500.0
    heur={
        'schema_version':1,
        'source_sha256':digest,
        'attempt_id':a2['id'],
        'task_id':t2,
        'published_at':pub2,
        'video_version_id':version_id(digest,pub2),
        'truth_status':'heuristic_only_not_play_or_swing_truth',
        'source_duration_ms':10000,
        'clips':[{'clip_id':'clip_1','original_start_ms':1000,'original_end_ms':4000}],
        'removed_gaps':[{'gap_index':1,'original_start_ms':4000,'original_end_ms':10000}],
    }
    _write_process_attempt_bundle(cfg,t2,a2['id'],digest,published_at=pub2,heuristics=heur)
    assert c.post('/api/v1/attempts/'+a2['id']+'/complete',json={'task_id':t2,'lease_token':a2['lease_token'],'final_event_sequence':0,'source_sha256':digest,'result_ref':{'storage_id':'processing','path':f'runs/{t2}/{a2["id"]}/result.json'}},headers=dict(wh,**{'Idempotency-Key':'c2'})).status_code==200

    # Simulate crash halfway through stage-candidate (after media rename, during candidate manifest write)
    import files as files_mod
    real_atomic=files_mod.atomic
    crashed=[]
    def crash_once(path,val):
        if 'candidates' in str(path) and not crashed:
            crashed.append(True)
            raise OSError('simulated staging crash')
        return real_atomic(path,val)
    monkeypatch.setattr(files_mod,'atomic',crash_once)
    assert c.post('/api/v1/tasks/'+t2+'/stage-candidate',json={},headers=h).status_code==503
    monkeypatch.setattr(files_mod,'atomic',real_atomic)

    # Replay stage-candidate (and verify active session still points to a1!)
    stage_res=c.post('/api/v1/tasks/'+t2+'/stage-candidate',json={},headers=h)
    assert stage_res.status_code==200,stage_res.data
    assert stage_res.json['media_state']=='staged_candidate'
    lib=Path(cfg['storage']['library']['path'])
    assert json.loads((lib/'metadata/sessions'/f'{digest}.json').read_text())['processing']['attempt_id']==a1['id']
    assert (lib/'metadata/candidates'/digest/f'{a2["id"]}.json').exists()

    # Check output-version catalog and strict source_id (32-hex) vs source_sha256 (64-hex) separation
    assert c.get('/api/v1/sources/'+digest+'/versions').status_code==404
    assert c.get('/api/v1/videos/'+s+'/versions').status_code==400
    versions=c.get('/api/v1/videos/'+digest+'/versions').json['items']
    assert len(versions)==2
    assert versions[0]['attempt_id']==a1['id'] and versions[0]['is_active'] is True and versions[0]['media_state']=='available'
    assert versions[1]['attempt_id']==a2['id'] and versions[1]['is_active'] is False and versions[1]['media_state']=='staged_candidate'
    assert versions[1]['has_heuristics'] is True
    assert versions[1]['id']==version_id(digest,pub2)
    assert versions[1]['source_id']==s and versions[1]['source_sha256']==digest
    assert c.get('/api/v1/attempts/'+a2['id']+'/heuristics').json['video_version_id']==version_id(digest,pub2)

    # Activate the already-staged candidate t2 -> replay-safe activation without re-moving media
    act_res=c.post('/api/v1/tasks/'+t2+'/activate',json={'expected_active_attempt':a1['id']},headers=h)
    assert act_res.status_code==200 and act_res.json['attempt_id']==a2['id'] and act_res.json['media_state']=='available'
    assert json.loads((lib/'metadata/sessions'/f'{digest}.json').read_text())['processing']['attempt_id']==a2['id']
    # Previous serving attempt a1 now has permanent rollback_previous pin (expires_at is None)
    a1_pins=c.get('/api/v1/attempts/'+a1['id']+'/pins').json
    assert 'rollback_previous' in a1_pins['pin_summary']['holder_kinds']
    rollback_pin=next(p for p in a1_pins['items'] if p['holder_kind']=='rollback_previous')
    assert rollback_pin['expires_at'] is None


def test_empty_candidate_acceptance_staging_pinning_and_activation_rejection(env):
    app,c,h,s,cfg=env
    w,wh=worker(c)
    digest=hashlib.sha256(b'input').hexdigest()
    t=task(c,h,s,'process','empty-cand')
    a=claim(c,w,wh)['attempt']
    heur={
        'schema_version':1,
        'source_sha256':digest,
        'attempt_id':a['id'],
        'task_id':t,
        'published_at':1700000900.0,
        'truth_status':'heuristic_only_not_play_or_swing_truth',
        'source_duration_ms':10000,
        'clips':[],
        'removed_gaps':[{'gap_index':1,'original_start_ms':0,'original_end_ms':10000}],
    }
    _write_process_attempt_bundle(cfg,t,a['id'],digest,published_at=1700000900.0,clips=[],heuristics=heur)
    done={'task_id':t,'lease_token':a['lease_token'],'final_event_sequence':0,'source_sha256':digest,'result_ref':{'storage_id':'processing','path':f'runs/{t}/{a["id"]}/result.json'}}
    assert c.post('/api/v1/attempts/'+a['id']+'/complete',json=done,headers=dict(wh,**{'Idempotency-Key':'done-empty'})).status_code==200

    # Stage empty candidate -> creates empty media dir and candidate manifest, media_state='staged_candidate'
    st=c.post('/api/v1/tasks/'+t+'/stage-candidate',json={},headers=h)
    assert st.status_code==200 and st.json['media_state']=='staged_candidate'
    lib=Path(cfg['storage']['library']['path'])
    assert (lib/'media'/a['id']).is_dir()
    assert (lib/'metadata/candidates'/digest/f'{a["id"]}.json').is_file()

    # Catalog includes empty staged candidate for comparison descriptors with playable=False, clip_count=0
    versions=c.get('/api/v1/videos/'+digest+'/versions').json['items']
    assert len(versions)==1 and versions[0]['attempt_id']==a['id']
    assert versions[0]['media_state']=='staged_candidate' and versions[0]['playable'] is False and versions[0]['clip_count']==0

    # Pinning empty candidate for comparison_session works and confirms media_state
    pin_res=c.post('/api/v1/attempts/'+a['id']+'/pins',json={'holder_kind':'comparison_session','holder_id':'b'*32,'ttl_seconds':600},headers=h)
    assert pin_res.status_code==200 and pin_res.json['media_state']=='staged_candidate' and pin_res.json['pinned'] is True

    # Activating an empty candidate for serving is rejected!
    act_res=c.post('/api/v1/tasks/'+t+'/activate',json={'expected_active_attempt':None},headers=h)
    assert act_res.status_code==400 and act_res.json['error']['code']=='empty_clips_cannot_activate'


def test_worker_heuristics_size_and_contract_validation(env):
    app,c,h,s,cfg=env
    w,wh=worker(c)
    digest=hashlib.sha256(b'input').hexdigest()

    # 1. Oversized heuristic-diagnostics.json > 512 KiB rejected
    t1=task(c,h,s,'process','heur-big')
    a1=claim(c,w,wh)['attempt']
    big_bytes=b'{"schema_version":1,"pad":"' + (b'x'*(520*1024)) + b'"}'
    _write_process_attempt_bundle(cfg,t1,a1['id'],digest,heuristics=big_bytes)
    r1=c.post('/api/v1/attempts/'+a1['id']+'/complete',json={'task_id':t1,'lease_token':a1['lease_token'],'final_event_sequence':0,'source_sha256':digest,'result_ref':{'storage_id':'processing','path':f'runs/{t1}/{a1["id"]}/result.json'}},headers=dict(wh,**{'Idempotency-Key':'h-big'}))
    assert r1.status_code==400 and r1.json['error']['code']=='heuristic_diagnostics_too_large'
    c.post('/api/v1/attempts/'+a1['id']+'/fail',json={'task_id':t1,'lease_token':a1['lease_token'],'final_event_sequence':0,'error_code':'abort','retryable':False},headers=dict(wh,**{'Idempotency-Key':'f-big'}))

    # 2. Forbidden truth claim in heuristics rejected
    t2=task(c,h,s,'process','heur-truth')
    a2=claim(c,w,wh)['attempt']
    bad_heur={'schema_version':1,'source_sha256':digest,'attempt_id':a2['id'],'truth_status':'heuristic_only_not_play_or_swing_truth','verified_play':True,'clips':[],'removed_gaps':[]}
    _write_process_attempt_bundle(cfg,t2,a2['id'],digest,heuristics=bad_heur)
    r2=c.post('/api/v1/attempts/'+a2['id']+'/complete',json={'task_id':t2,'lease_token':a2['lease_token'],'final_event_sequence':0,'source_sha256':digest,'result_ref':{'storage_id':'processing','path':f'runs/{t2}/{a2["id"]}/result.json'}},headers=dict(wh,**{'Idempotency-Key':'h-truth'}))
    assert r2.status_code==400 and r2.json['error']['code']=='forbidden_heuristic_truth_claim'
    c.post('/api/v1/attempts/'+a2['id']+'/fail',json={'task_id':t2,'lease_token':a2['lease_token'],'final_event_sequence':0,'error_code':'abort','retryable':False},headers=dict(wh,**{'Idempotency-Key':'f-truth'}))

    # 3. Mismatched clip_id or clip bounds against frozen session.clips rejected
    t3=task(c,h,s,'process','heur-mismatch')
    a3=claim(c,w,wh)['attempt']
    mismatch_heur={'schema_version':1,'source_sha256':digest,'attempt_id':a3['id'],'truth_status':'heuristic_only_not_play_or_swing_truth','source_duration_ms':10000,'clips':[{'clip_id':'wrong_id','original_start_ms':1000,'original_end_ms':4000}],'removed_gaps':[]}
    _write_process_attempt_bundle(cfg,t3,a3['id'],digest,heuristics=mismatch_heur)
    r3=c.post('/api/v1/attempts/'+a3['id']+'/complete',json={'task_id':t3,'lease_token':a3['lease_token'],'final_event_sequence':0,'source_sha256':digest,'result_ref':{'storage_id':'processing','path':f'runs/{t3}/{a3["id"]}/result.json'}},headers=dict(wh,**{'Idempotency-Key':'h-mismatch'}))
    assert r3.status_code==400 and r3.json['error']['code']=='heuristic_clip_id_mismatch'


def test_pin_isolation_opaque_holders_and_fenced_quarantine_recovery(env):
    app,c,h,s,cfg=env
    store=app.config['STORE']
    files=app.config['FILES']
    clock=[2000.0]
    store.clock=lambda:clock[0]
    w,wh=worker(c)
    digest=hashlib.sha256(b'input').hexdigest()

    t=task(c,h,s,'process','cand-pin')
    a=claim(c,w,wh)['attempt']
    _write_process_attempt_bundle(cfg,t,a['id'],digest)
    assert c.post('/api/v1/attempts/'+a['id']+'/complete',json={'task_id':t,'lease_token':a['lease_token'],'final_event_sequence':0,'source_sha256':digest,'result_ref':{'storage_id':'processing','path':f'runs/{t}/{a["id"]}/result.json'}},headers=dict(wh,**{'Idempotency-Key':'c-pin'})).status_code==200
    assert c.post('/api/v1/tasks/'+t+'/stage-candidate',json={},headers=h).status_code==200

    # Acquire two independent pins: comparison_session (32hex) and saved_comparison (cmp_<32hex>_<32hex>, 69 chars)
    sess_holder='a'*32
    saved_holder='cmp_'+'1'*32+'_'+'2'*32
    assert len(saved_holder)==69
    p1=c.post('/api/v1/attempts/'+a['id']+'/pins',json={'holder_kind':'comparison_session','holder_id':sess_holder,'ttl_seconds':300},headers=h)
    p2=c.post('/api/v1/attempts/'+a['id']+'/pins',json={'holder_kind':'saved_comparison','holder_id':saved_holder},headers=h)
    assert p1.status_code==200 and p2.status_code==200
    assert p2.json['pin_summary']['active_pin_count']==2

    # Release comparison_session via DELETE /attempts/<id>/pins with JSON body; saved_comparison pin remains isolated!
    del1=c.delete('/api/v1/attempts/'+a['id']+'/pins',json={'holder_kind':'comparison_session','holder_id':sess_holder},headers=h)
    assert del1.status_code==200 and del1.json['pinned'] is True and del1.json['media_state']=='staged_candidate'
    assert del1.json['pin_summary']['holder_kinds']==['saved_comparison']

    # Release saved_comparison; with retention disabled by default, attempt stays staged_candidate and sweep evicts nothing
    del2=c.delete('/api/v1/attempts/'+a['id']+'/pins',json={'holder_kind':'saved_comparison','holder_id':saved_holder},headers=h)
    assert del2.status_code==200 and del2.json['pinned'] is False and del2.json['media_state']=='staged_candidate'
    sweep_off=c.post('/api/v1/retention/sweep',json={},headers=h).json
    assert sweep_off['enabled'] is False and sweep_off['evicted']==[]

    # Enable retention (default 14-day candidate TTL + 60s grace period):
    # Before 14-day TTL elapses, unpinned candidate stays staged_candidate (not immediately gc_eligible!)
    cfg['retention']={'enabled':True,'grace_period_seconds':60}
    store.retention=cfg['retention']
    assert c.get('/api/v1/attempts/'+a['id']+'/pins').json['media_state']=='staged_candidate'
    assert c.post('/api/v1/retention/sweep',json={},headers=h).json['evicted']==[]

    # Advance clock past 14-day candidate TTL -> becomes gc_eligible; re-pinning clears gc_eligible_since and unpinning starts fresh grace
    clock[0]=2000.0+14*86400+1
    first_elig=c.get('/api/v1/attempts/'+a['id']+'/pins').json
    assert first_elig['media_state']=='gc_eligible' and first_elig['pin_summary']['gc_eligible_since']==clock[0]
    clock[0]+=10.0
    assert c.get('/api/v1/attempts/'+a['id']+'/pins').json['pin_summary']['gc_eligible_since']==first_elig['pin_summary']['gc_eligible_since']
    repin=c.post('/api/v1/attempts/'+a['id']+'/pins',json={'holder_kind':'operator_manual','holder_id':'op1'},headers=h).json
    assert repin['media_state']=='staged_candidate' and repin['pin_summary']['gc_eligible_since'] is None
    clock[0]+=20.0
    unpin_res=c.delete('/api/v1/attempts/'+a['id']+'/pins/operator_manual/op1',headers={'X-Tennis-Request':'1'}).json
    assert unpin_res['media_state']=='gc_eligible' and unpin_res['pin_summary']['gc_eligible_since']==clock[0]

    # Advance past 60s grace period and run retention sweep -> evicted
    clock[0]+=100.0
    sweep_on=c.post('/api/v1/retention/sweep',json={},headers=h).json
    assert a['id'] in sweep_on['evicted']
    lib=Path(cfg['storage']['library']['path'])
    assert not (lib/'media'/a['id']).exists()
    assert (lib/'metadata/runs'/a['id']/'session.json').exists()

    # Media fences: pinning, activating, or snapshotting an evicted attempt returns 409 media_already_evicted
    assert c.post('/api/v1/attempts/'+a['id']+'/pins',json={'holder_kind':'operator_manual','holder_id':'op2'},headers=h).status_code==409
    assert c.post('/api/v1/tasks/'+t+'/activate',json={'expected_active_attempt':None},headers=h).status_code==409
    assert c.post('/api/v1/feedback-snapshots',json=_valid_snapshot_payload(digest,a['id']),headers=dict(h,**{'Idempotency-Key':'snap-evicted'})).status_code==409

    # Test crash recovery and pre-quarantine safety for 'quarantined' / symlink / wrong metadata attempts:
    # Case A: Both original and .trash exist -> conflict! Must NOT delete either or mark evicted!
    t_conf=task(c,h,s,'process','cand-conflict')
    a_conf=claim(c,w,wh)['attempt']
    _write_process_attempt_bundle(cfg,t_conf,a_conf['id'],digest)
    c.post('/api/v1/attempts/'+a_conf['id']+'/complete',json={'task_id':t_conf,'lease_token':a_conf['lease_token'],'final_event_sequence':0,'source_sha256':digest,'result_ref':{'storage_id':'processing','path':f'runs/{t_conf}/{a_conf["id"]}/result.json'}},headers=dict(wh,**{'Idempotency-Key':'c-conf'}))
    c.post('/api/v1/tasks/'+t_conf+'/stage-candidate',json={},headers=h)
    with store.tx() as db:
        db.execute("UPDATE attempts SET media_state='quarantined' WHERE id=?",(a_conf['id'],))
    trash_conflict=lib/'media/.trash'/a_conf['id']
    trash_conflict.mkdir(parents=True,exist_ok=True)
    (trash_conflict/'partial.mp4').write_bytes(b'partial')
    rec=files.recover_quarantined(store)
    assert a_conf['id'] in rec['conflicts']
    assert (lib/'media'/a_conf['id']/'Practice.mp4').exists()
    assert c.get('/api/v1/tasks/'+t_conf).json['media_state']=='quarantined'
    assert c.post('/api/v1/attempts/'+a_conf['id']+'/pins',json={'holder_kind':'operator_manual','holder_id':'op3'},headers=h).status_code==409

    # Once operator resolves the duplicate .trash dir, recovery completes eviction cleanly
    import shutil
    shutil.rmtree(trash_conflict)
    rec2=files.recover_quarantined(store)
    assert a_conf['id'] in rec2['recovered']
    assert not (lib/'media'/a_conf['id']).exists()
    assert c.get('/api/v1/tasks/'+t_conf).json['media_state']=='evicted'

    # Case B: Internal symlink pointing to another attempt inside library/media + wrong/missing metadata
    t_victim=task(c,h,s,'process','cand-victim')
    a_victim=claim(c,w,wh)['attempt']
    _write_process_attempt_bundle(cfg,t_victim,a_victim['id'],digest)
    c.post('/api/v1/attempts/'+a_victim['id']+'/complete',json={'task_id':t_victim,'lease_token':a_victim['lease_token'],'final_event_sequence':0,'source_sha256':digest,'result_ref':{'storage_id':'processing','path':f'runs/{t_victim}/{a_victim["id"]}/result.json'}},headers=dict(wh,**{'Idempotency-Key':'c-victim'}))
    c.post('/api/v1/tasks/'+t_victim+'/stage-candidate',json={},headers=h)
    c.post('/api/v1/attempts/'+a_victim['id']+'/pins',json={'holder_kind':'operator_manual','holder_id':'keep-victim'},headers=h)

    t_sym=task(c,h,s,'process','cand-sym')
    a_sym=claim(c,w,wh)['attempt']
    _write_process_attempt_bundle(cfg,t_sym,a_sym['id'],digest)
    c.post('/api/v1/attempts/'+a_sym['id']+'/complete',json={'task_id':t_sym,'lease_token':a_sym['lease_token'],'final_event_sequence':0,'source_sha256':digest,'result_ref':{'storage_id':'processing','path':f'runs/{t_sym}/{a_sym["id"]}/result.json'}},headers=dict(wh,**{'Idempotency-Key':'c-sym'}))
    c.post('/api/v1/tasks/'+t_sym+'/stage-candidate',json={},headers=h)
    # Replace library/media/<a_sym> with an internal symlink pointing to library/media/<a_victim>!
    shutil.rmtree(lib/'media'/a_sym['id'])
    (lib/'media'/a_sym['id']).symlink_to(lib/'media'/a_victim['id'])
    clock[0]+=14*86400+100.0
    assert c.post('/api/v1/retention/sweep',json={},headers=h).json['evicted']==[]  # First transition starts grace now.
    clock[0]+=100.0
    sweep_sym=c.post('/api/v1/retention/sweep',json={},headers=h).json
    assert a_sym['id'] in sweep_sym['conflicts']
    assert a_sym['id'] not in sweep_sym['evicted']
    assert (lib/'media'/a_victim['id']/'Practice.mp4').exists()
    assert c.get('/api/v1/tasks/'+t_sym).json['media_state']!='evicted'

    # Case C: Tampered/mismatched metadata/runs/<attempt>/session.json blocks quarantine and recovery
    (lib/'media'/a_sym['id']).unlink()
    (lib/'media'/a_sym['id']).mkdir(parents=True)
    (lib/'media'/a_sym['id']/'Practice.mp4').write_bytes(b'sym-own-media')
    bad_sess=json.loads((lib/'metadata/runs'/a_sym['id']/'session.json').read_text())
    bad_sess['processing']['attempt_id']='wrong_attempt_id'
    (lib/'metadata/runs'/a_sym['id']/'session.json').write_text(json.dumps(bad_sess))
    sweep_bad_meta=c.post('/api/v1/retention/sweep',json={},headers=h).json
    assert a_sym['id'] in sweep_bad_meta['conflicts']
    assert (lib/'media'/a_sym['id']/'Practice.mp4').exists()
    assert c.get('/api/v1/tasks/'+t_sym).json['media_state']!='evicted'


def test_legacy_attempts_migrate_unknown_not_available(env):
    from legacy import import_library
    app,c,h,s,cfg=env
    store=app.config['STORE']
    files=app.config['FILES']
    root=Path(cfg['storage']['library']['path'])/'metadata/sessions'
    root.mkdir(parents=True,exist_ok=True)
    digest=hashlib.sha256(b'input').hexdigest()
    (root/(digest+'.json')).write_text(json.dumps({'id':digest,'published_at':1690000000.0,'source':{'name':'test.mov','duration_seconds':10.0},'algorithm':'legacy'}))
    assert import_library(store,files)['imported']==1
    legacy_task=store.tasks()[0]
    detail=store.detail(legacy_task['id'])
    legacy_attempt=detail['attempts'][0]
    assert legacy_attempt['media_state']=='unknown'
    # Unknown legacy attempt is NOT returned as a playable version in GET /api/v1/videos/<digest>/versions
    assert c.get('/api/v1/videos/'+digest+'/versions').json['items']==[]
    # Unknown legacy attempt cannot be pinned
    assert c.post('/api/v1/attempts/'+legacy_attempt['id']+'/pins',json={'holder_kind':'operator_manual','holder_id':'op-leg'},headers=h).status_code==409


def test_portal_stage_for_comparison_and_task_pin_media_state_exposure(env):
    app,c,h,s,cfg=env
    js=c.get('/static/app.js').text
    assert 'Stage for comparison' in js
    assert '/stage-candidate' in js
    assert 'media_state' in js and 'pin_summary' in js
    w,wh=worker(c)
    digest=hashlib.sha256(b'input').hexdigest()
    t=task(c,h,s,'process','portal-cand')
    a=claim(c,w,wh)['attempt']
    _write_process_attempt_bundle(cfg,t,a['id'],digest)
    assert c.post('/api/v1/attempts/'+a['id']+'/complete',json={'task_id':t,'lease_token':a['lease_token'],'final_event_sequence':0,'source_sha256':digest,'result_ref':{'storage_id':'processing','path':f'runs/{t}/{a["id"]}/result.json'}},headers=dict(wh,**{'Idempotency-Key':'c-portal'})).status_code==200
    assert c.post('/api/v1/tasks/'+t+'/stage-candidate',json={},headers=h).status_code==200
    assert c.post('/api/v1/attempts/'+a['id']+'/pins',json={'holder_kind':'comparison_session','holder_id':'c'*32,'ttl_seconds':300},headers=h).status_code==200
    tasks_list=c.get('/api/v1/tasks').json['items']
    row=next(item for item in tasks_list if item['id']==t)
    assert row['media_state']=='staged_candidate'
    assert row['pin_summary']['pinned'] is True and 'comparison_session' in row['pin_summary']['holder_kinds']
    detail=c.get('/api/v1/tasks/'+t).json
    assert detail['attempts'][0]['media_state']=='staged_candidate'
    assert detail['attempts'][0]['pin_summary']['active_pin_count']==1


def test_cleanup_enabled_for_old_candidate_still_starts_a_full_grace_window(env):
    app,c,h,s,cfg=env;store=app.config['STORE'];files=app.config['FILES'];clock=[1000.0];store.clock=lambda:clock[0]
    w,wh=worker(c);digest=hashlib.sha256(b'input').hexdigest();t=task(c,h,s,'process','old-candidate');a=claim(c,w,wh)['attempt']
    _write_process_attempt_bundle(cfg,t,a['id'],digest)
    result={'task_id':t,'lease_token':a['lease_token'],'final_event_sequence':0,'source_sha256':digest,'result_ref':{'storage_id':'processing','path':f'runs/{t}/{a["id"]}/result.json'}}
    assert c.post('/api/v1/attempts/'+a['id']+'/complete',json=result,headers=dict(wh,**{'Idempotency-Key':'old-complete'})).status_code==200
    assert c.post('/api/v1/tasks/'+t+'/stage-candidate',json={},headers=h).status_code==200
    clock[0]+=40*86400;cfg['retention']={'enabled':True};store.retention=cfg['retention']
    # A long-offline server or newly enabled cleanup must not backdate grace.
    first=files.run_retention(store);assert first['evicted']==[]
    with store.tx() as db:assert db.execute('SELECT gc_eligible_since FROM attempts WHERE id=?',(a['id'],)).fetchone()[0]==clock[0]
    clock[0]+=86400-1;assert files.run_retention(store)['evicted']==[]
    clock[0]+=2;assert a['id'] in files.run_retention(store)['evicted']


def test_version_catalog_is_paginated_and_supports_exact_version_lookup(env):
    app,c,h,s,cfg=env;w,wh=worker(c);digest=hashlib.sha256(b'input').hexdigest()
    for i in range(2):
        t=task(c,h,s,'process','catalog-'+str(i));a=claim(c,w,wh)['attempt'];_write_process_attempt_bundle(cfg,t,a['id'],digest,published_at=1700000000.0+i)
        r=c.post('/api/v1/attempts/'+a['id']+'/complete',json={'task_id':t,'lease_token':a['lease_token'],'final_event_sequence':0,'source_sha256':digest,'result_ref':{'storage_id':'processing','path':f'runs/{t}/{a["id"]}/result.json'}},headers=dict(wh,**{'Idempotency-Key':'catalog-complete-'+str(i)}));assert r.status_code==200
        assert c.post('/api/v1/tasks/'+t+'/stage-candidate',json={},headers=h).status_code==200
    first=c.get('/api/v1/videos/'+digest+'/versions?limit=1').json;assert len(first['items'])==1 and first['next_cursor']==1
    second=c.get('/api/v1/videos/'+digest+'/versions?limit=1&cursor=1').json;assert len(second['items'])==1 and second['next_cursor'] is None
    assert first['items'][0]['id']!=second['items'][0]['id']
    exact=c.get('/api/v1/videos/'+digest+'/versions?video_version_id='+second['items'][0]['id']).json;assert exact['items'][0]['id']==second['items'][0]['id'] and len(exact['items'])==1


def test_parallel_startup_migrates_legacy_attempts_without_losing_history(tmp_path):
    import sqlite3
    from store import Store
    path=tmp_path/'legacy.sqlite3'
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE attempts(id TEXT PRIMARY KEY,task_id TEXT,worker_id TEXT,token TEXT,expires REAL,status TEXT,number INTEGER,body TEXT,result TEXT)')
        db.execute('INSERT INTO attempts VALUES(?,?,?,?,?,?,?,?,?)',('old','task','worker','fixture-token',1234,'running',1,'{"preserved":true}',None))
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:list(pool.map(lambda _:Store(path),range(4)))
    with sqlite3.connect(path) as db:
        row=db.execute('SELECT id,body,media_state FROM attempts').fetchone();assert row==('old','{"preserved":true}','unknown')
        assert db.execute('SELECT COUNT(*) FROM attempts').fetchone()[0]==1


def test_boundary_override_cannot_erase_an_unrelated_base_clip():
    from feedback import validate_snapshot,InvalidFeedback
    digest='a'*64;attempt='b'*32;p=_valid_snapshot_payload(digest,attempt)
    p['selected_evidence_revisions']=p['selected_evidence_revisions'][:1]
    base={'id':digest,'published_at':1700000000.0,'processing':{'attempt_id':attempt},'source':{'sha256':digest,'duration_seconds':10},'clips':[{'id':'clip_1','original_start_seconds':1,'original_end_seconds':4},{'id':'clip_2','original_start_seconds':6,'original_end_seconds':9}]}
    for a,b in ((2000,8000),(7000,8000),(4000,5000)):
        p['confirmed_commands']={'keep_intervals_ms':[],'exclude_intervals_ms':[],'override_clip_bounds_ms':[dict(clip_id='clip_1',original_start_ms=a,original_end_ms=b,source_annotation_id='ann_clip_1',source_revision=1)]}
        with pytest.raises(InvalidFeedback,match='override_crosses_base_clip'):validate_snapshot(p,base)
    p['confirmed_commands']['override_clip_bounds_ms'][0].update(original_start_ms=1200,original_end_ms=4500)
    assert validate_snapshot(p,base)['confirmed_commands']['override_clip_bounds_ms'][0]['original_end_ms']==4500
