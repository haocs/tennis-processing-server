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
