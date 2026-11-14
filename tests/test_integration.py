"""Real stage/export/upload dryrun integration with deterministic AI decisions."""
import hashlib
import importlib
import json
from pathlib import Path
import sys
import pytest


def test_real_worker_dryrun_and_process(env,monkeypatch,tmp_path):
    app,client,headers,source_id,config=env
    processor=Path(__file__).resolve().parents[2]/'tennis-processing'
    if not (processor/'pipeline.py').exists(): pytest.skip('Run integration with sibling tennis-processing checkout')
    pytest.importorskip('av');pytest.importorskip('imageio_ffmpeg');pytest.importorskip('numpy')
    sys.path.insert(0,str(processor))
    from api_worker import Agent,API,APIError
    import pipeline
    pipeline.run_media(['-f','lavfi','-i','testsrc2=size=160x90:rate=30','-t','2','-c:v','libx264','-metadata','make=TestCamera',str(tmp_path/'fixture.mov')],tmp_path/'ffmpeg.log')
    original=Path(config['storage']['originals']['path'])/'test.mov';original.write_bytes((tmp_path/'fixture.mov').read_bytes())
    source_id=client.post('/api/v1/sources',json={'storage_id':'originals','path':'test.mov'},headers=headers).json['source_id']
    def call(api,path,body=None,key=None,bootstrap=False):
        h={'Authorization':'Bearer '+('test-secret' if bootstrap else api.state.get('worker_token','')),'X-Worker-ID':api.state.get('worker_id','')}
        if key:h['Idempotency-Key']=key
        result=client.open('/api/v1'+path,method='GET' if body is None else 'POST',json=body,headers=h)
        if result.status_code>=400:raise APIError(result.status_code,result.text)
        return result.json if result.status_code!=204 else None
    monkeypatch.setattr(API,'call',call)
    monkeypatch.setattr(pipeline,'analyze_video',lambda *args:([[.2,1.5]],{'detector':'test','target_visible_fraction':1}))
    worker_cfg={'state_dir':str(tmp_path/'worker'),'server_url':'http://test','registration_token':'test-secret','name':'Integration','storage':config['storage'],'processor':{}}
    agent=Agent(worker_cfg)
    import hashlib
    for kind in ('dryrun','process'):
        task=client.post('/api/v1/tasks',json={'source_id':source_id,'type':kind},headers=dict(headers,**{'Idempotency-Key':kind})).json['task_id']
        assignment=agent.api.call('/workers/'+agent.state['worker_id']+'/claims',{'request_id':kind})
        agent.execute(assignment)
        detail=client.get('/api/v1/tasks/'+task).json
        assert detail['status']=='succeeded',detail
        assert detail['result']['timing']['total_seconds']>0
        if kind=='dryrun':
            assert detail['result']['result_ref'] is None
            assert not (Path(config['storage']['processing']['path'])/'runs').exists()
            assert list((tmp_path/'worker/tasks'/task/'bundle/media').glob('*.mp4'))
        else:
            path=app.config['FILES'].resolve(detail['result']['result_ref'])
            result=json.loads(path.read_text());assert result['session']['source']['sha256']==hashlib.sha256(original.read_bytes()).hexdigest()
            assert (path.parent/'metadata/original-metadata.json').exists()
            assert client.post('/api/v1/tasks/'+task+'/activate',json={'expected_active_attempt':None},headers=headers).status_code==200


def test_real_worker_hybrid_ai_vision_and_fallback(env,monkeypatch,tmp_path):
    app,client,headers,_,config=env
    processor=Path(__file__).resolve().parents[2]/'tennis-processing'
    if not (processor/'hybrid_ai.py').exists(): pytest.skip('Run integration with sibling tennis-processing checkout')
    pytest.importorskip('av');pytest.importorskip('imageio_ffmpeg');pytest.importorskip('numpy');pytest.importorskip('PIL')
    sys.path.insert(0,str(processor))
    from api_worker import Agent,API,APIError
    import detector,hybrid_ai,pipeline
    np=pytest.importorskip('numpy')

    pipeline.run_media(['-f','lavfi','-i','testsrc2=size=160x90:rate=30','-t','2','-c:v','libx264','-metadata','make=TestCamera',str(tmp_path/'hybrid.mov')],tmp_path/'ffmpeg.log')
    original=Path(config['storage']['originals']['path'])/'hybrid.mov';original.write_bytes((tmp_path/'hybrid.mov').read_bytes())
    source_id=client.post('/api/v1/sources',json={'storage_id':'originals','path':'hybrid.mov'},headers=headers).json['source_id']

    def call(api,path,body=None,key=None,bootstrap=False):
        h={'Authorization':'Bearer '+('test-secret' if bootstrap else api.state.get('worker_token','')),'X-Worker-ID':api.state.get('worker_id','')}
        if key:h['Idempotency-Key']=key
        result=client.open('/api/v1'+path,method='GET' if body is None else 'POST',json=body,headers=h)
        if result.status_code>=400:raise APIError(result.status_code,result.text)
        return result.json if result.status_code!=204 else None
    monkeypatch.setattr(API,'call',call)

    def fake_extract(video_path,cfg):
        records=[]
        for idx in range(16):
            t=round(idx*0.125,6)
            active=0.2<=t<=1.5
            phase=1.0 if idx%2==0 else -1.0
            kp=np.zeros((1,17,2),dtype=np.float32)
            kc=np.zeros((1,17),dtype=np.float32)
            kp[0,5]=[60.0,30.0];kp[0,6]=[100.0,30.0]
            kp[0,11]=[65.0,70.0];kp[0,12]=[95.0,70.0]
            kp[0,9]=[55.0,50.0+(phase*25.0 if active else 0.0)]
            kp[0,10]=[105.0+(phase*22.0 if active else 0.0),48.0]
            kc[0,[5,6,9,10,11,12]]=0.95
            boxes=np.array([[45.0,20.0,115.0,85.0]],dtype=np.float32)
            records.append({'t':t,'boxes':boxes,'k':kp,'kc':kc})
        return records,{'width':160,'height':90,'target_fps':8.0,'actual_fps':8.0,'pts_timebase':'1/1000','frame_count':len(records)},2.0
    monkeypatch.setattr(detector,'extract_pose_records',fake_extract)

    invoke_models=[]
    def fake_invoke(*,model_id,batch,tier,prompt,batch_dir):
        invoke_models.append(model_id)
        if model_id=='gemini:gemini-2.5-flash':
            raise hybrid_ai.ProviderCallError('rate_limit','429 synthetic quota')
        return {'decisions':[{'candidate_id':c['candidate_id'],'action':'keep','category':'rally','confidence':0.93,'reason':'rally'} for c in batch]}

    worker_cfg={
        'state_dir':str(tmp_path/'worker'),
        'server_url':'http://test',
        'registration_token':'test-secret',
        'name':'HybridIntegration',
        'storage':config['storage'],
        'processor':{'padding_before':0.2,'padding_after':0.2,'merge_gap':0.3,'_ai_provider_invoke':fake_invoke},
        'ai':{'enabled':True,'providers':['gemini'],'gemini_api_key':'AIzaSyntheticIntegrationKey123456','gemini_models':['gemini-2.5-flash','gemini-2.5-flash-lite'],'default_model':'gemini:gemini-2.5-flash'},
    }
    agent=Agent(worker_cfg)
    workers_resp=client.get('/api/v1/workers').json
    me=next(w for w in workers_resp['items'] if w['id']==agent.state['worker_id'])
    assert 'hybrid-ai-vision-v1' in me['capabilities']
    assert [m['id'] for m in workers_resp['available_ai_models']]==['gemini:gemini-2.5-flash','gemini:gemini-2.5-flash-lite']

    task=client.post('/api/v1/tasks',json={
        'source_id':source_id,
        'type':'process',
        'parameters':{
            'processing_strategy':'hybrid_ai_vision',
            'ai_model':'gemini:gemini-2.5-flash',
            'allow_model_fallback':True,
            'ai_tuning':{'aggressiveness':'balanced','accuracy_tier':'fast'},
        },
    },headers=dict(headers,**{'Idempotency-Key':'hybrid-int-1'})).json['task_id']
    assignment=agent.api.call('/workers/'+agent.state['worker_id']+'/claims',{'request_id':'hybrid-claim-1'})
    assert assignment and assignment['task']['id']==task
    agent.execute(assignment)

    detail=client.get('/api/v1/tasks/'+task).json
    assert detail['status']=='succeeded',detail
    ai_ref=detail['result']['findings']['ai_refinement']
    assert ai_ref['requested_ai_model']=='gemini:gemini-2.5-flash'
    assert ai_ref['actual_ai_model']=='gemini:gemini-2.5-flash-lite'
    assert ai_ref['fallback_used'] is True
    assert ai_ref['provenance_artifact']=='ai-provenance.json'
    assert invoke_models==['gemini:gemini-2.5-flash','gemini:gemini-2.5-flash-lite']

    res_path=app.config['FILES'].resolve(detail['result']['result_ref'])
    res_manifest=json.loads(res_path.read_text())
    assert 'ai-provenance.json' in res_manifest['session']['artifacts']
    prov_path=res_path.parent/'metadata/ai-provenance.json'
    assert prov_path.is_file()
    assert hashlib.sha256(prov_path.read_bytes()).hexdigest()==ai_ref['provenance_sha256']

    assert client.post('/api/v1/tasks/'+task+'/stage-candidate',json={},headers=headers).status_code==200
    assert client.post('/api/v1/tasks/'+task+'/activate',json={'expected_active_attempt':None},headers=headers).status_code==200

    # Strict allow_model_fallback=False with unavailable exact model must not claim on this worker.
    pinned_task=client.post('/api/v1/tasks',json={
        'source_id':source_id,
        'type':'process',
        'parameters':{
            'processing_strategy':'hybrid_ai_vision',
            'ai_model':'antigravity:unavailable-model',
            'allow_model_fallback':False,
            'ai_tuning':{'aggressiveness':'balanced','accuracy_tier':'fast'},
        },
    },headers=dict(headers,**{'Idempotency-Key':'hybrid-int-pin'})).json['task_id']
    assert pinned_task
    assert agent.api.call('/workers/'+agent.state['worker_id']+'/claims',{'request_id':'hybrid-claim-pin'}) is None

    # End-to-end worker scratch cleanup removes processed task folder and reports confirmation via heartbeat while preserving cache.
    worker_tasks_dir=tmp_path/'worker'/'tasks'/task
    worker_cache_file=tmp_path/'worker'/'cache'/'pose-test.json'
    worker_cache_file.parent.mkdir(parents=True,exist_ok=True)
    worker_cache_file.write_text('{"cached":true}')
    assert worker_tasks_dir.is_dir()
    assert client.post('/api/v1/workers/'+agent.state['worker_id']+'/cleanup',json={},headers=headers).status_code==200
    agent.heartbeat()
    assert not worker_tasks_dir.exists()
    assert worker_cache_file.is_file()
    worker_info=next(w for w in client.get('/api/v1/workers').json['items'] if w['id']==agent.state['worker_id'])
    assert worker_info['last_cleanup']['removed_tasks']>=1
    assert worker_info['scratch']['cleanable_count']==0
    assert worker_info['scratch']['cleanable_bytes']==0
