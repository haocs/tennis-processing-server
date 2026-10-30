import concurrent.futures
import hashlib
import json
import sys
from pathlib import Path
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from app import create_app


@pytest.fixture
def env(tmp_path):
    roots={}
    for name in ('originals','processing','library'):
        root=tmp_path/name;root.mkdir();roots[name]={'path':str(root)}
    cfg={'data_dir':str(tmp_path/'data'),'registration_token':'test-secret','storage':roots}
    app=create_app(cfg);app.testing=True
    client=app.test_client();admin={'X-Tennis-Request':'1','Idempotency-Key':'task1'}
    path=Path(roots['originals']['path'])/'test.mov';path.write_bytes(b'input')
    source=client.post('/api/v1/sources',json={'storage_id':'originals','path':'test.mov'},headers=admin).json['source_id']
    return app,client,admin,source,cfg

