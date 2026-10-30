"""Safe storage references and publication by same-filesystem rename (no media recopy)."""
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import uuid
from store import Conflict, encode


def inside(root, path):
    if not isinstance(path,str) or not path or '\\' in path or ':' in path or path.startswith('/') or '..' in PurePosixPath(path).parts:
        raise ValueError('invalid_storage_path')
    root=Path(root).resolve()
    dest=(root/path).resolve()
    if not dest.is_relative_to(root) or dest==root: raise ValueError('invalid_storage_path')
    return dest


def atomic(path, value):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    temp=path.with_name('.'+path.name+'.'+uuid.uuid4().hex)
    temp.write_text(encode(value)+'\n');os.replace(temp,path)


class Files:
    def __init__(self, config):
        self.config=config

    def root(self, storage):
        spec=self.config['storage'][storage]
        root=Path(spec['path'])
        if not root.is_dir(): raise OSError('storage_unavailable')
        if spec.get('identity') and (root/spec.get('marker','.tennis-storage-id')).read_text().strip()!=spec['identity']:
            raise OSError('storage_identity_mismatch')
        return root

    def resolve(self, ref):
        return inside(self.root(ref['storage_id']),ref['path'])

    def validate_result(self, spec, body, attempt):
        if spec['type']=='dryrun':
            if body.get('result_ref'): raise ValueError('dryrun_result_must_be_local')
            return
        ref=body['result_ref']
        expected=f"runs/{body['task_id']}/{attempt}/result.json"
        if ref['storage_id']!='processing' or ref['path']!=expected: raise ValueError('result_outside_attempt')
        path=self.resolve(ref)
        if path.stat().st_size>8*1024*1024: raise ValueError('result_manifest_too_large')
        data=json.loads(path.read_text())
        if data['attempt_id']!=attempt or data['task_id']!=body['task_id'] or not re.fullmatch('[a-f0-9]{64}',data['source_sha256']): raise ValueError('invalid_result_identity')
        if not data.get('files'): raise ValueError('empty_result')
        for item in data['files']:
            asset=inside(path.parent,item['path'])
            if asset.stat().st_size!=item['bytes'] or not re.fullmatch('[a-f0-9]{64}',item['sha256']): raise ValueError('invalid_artifact')
        public=data['session']
        if public['id']!=data['source_sha256'] or not public.get('clips') or public['video']['file']!='Practice.mp4': raise ValueError('invalid_session')
        declared={a['path'] for a in data['files']}
        required={'media/Practice.mp4','media/poster.jpg','metadata/original-metadata.json'} | {'media/'+c['file'] for c in public['clips']} | {'metadata/'+a for a in public['artifacts']}
        if not required.issubset(declared): raise ValueError('missing_result_artifact')
        if not public.get('source',{}).get('sha256')==data['source_sha256']: raise ValueError('source_hash_mismatch')
        return data

    def activate(self, store, task_id, expected_active, policy='explicit'):
        import fcntl
        with open(store.path+'.publisher.lock','a') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX)
            return self._activate(store,task_id,expected_active,policy)

    def _activate(self, store, task_id, expected_active, policy):
        # Intent is committed BEFORE filesystem changes; a crash can be replayed.
        with store.tx() as db:
            task=db.execute('SELECT * FROM tasks WHERE id=?',(task_id,)).fetchone()
            if not task or task['status']!='succeeded': raise Conflict('task_not_completed')
            spec=json.loads(task['body'])
            if spec['type']!='process': raise ValueError('only_process_can_activate')
            result=json.loads(task['result']);attempt=task['attempt']
            path=self.resolve(result['result_ref']); data=json.loads(path.read_text());digest=data['source_sha256']
            current=db.execute('SELECT * FROM publications WHERE source_hash=?',(digest,)).fetchone()
            lib=self.root('library');manifest=lib/'metadata/sessions'/f'{digest}.json'
            active=current['attempt_id'] if current else ('legacy' if manifest.exists() else None)
            intent_row=db.execute("SELECT body FROM publication_intents WHERE task_id=? AND status='pending'",(task_id,)).fetchone()
            if current is None and manifest.exists() and intent_row:
                pending=json.loads(intent_row['body'])
                written=json.loads(manifest.read_text())
                if written.get('processing',{}).get('attempt_id')==attempt:
                    active=pending['expected_active']
            if active==attempt: return {'status':'active','attempt_id':attempt}
            if policy=='if_no_active_result' and active is not None:
                db.execute("UPDATE tasks SET publication='not_requested' WHERE id=?",(task_id,))
                return {'status':'not_requested','active_attempt':active}
            if active!=expected_active: raise Conflict('active_result_changed')
            intent={'expected_active':expected_active,'policy':policy,'attempt_id':attempt,'source_hash':digest}
            db.execute("INSERT OR REPLACE INTO publication_intents VALUES(?,?,'pending')",(task_id,encode(intent)))
            db.execute("UPDATE tasks SET publication='pending' WHERE id=?",(task_id,))
        media=lib/'media'/attempt;metadata=lib/'metadata/runs'/attempt
        media.parent.mkdir(parents=True,exist_ok=True);metadata.parent.mkdir(parents=True,exist_ok=True)
        source_media=path.parent/'media';source_meta=path.parent/'metadata'
        if not media.exists():
            if source_media.stat().st_dev!=media.parent.stat().st_dev: raise ValueError('publication_requires_same_filesystem')
            os.rename(source_media,media)
        if not metadata.exists(): os.rename(source_meta,metadata)
        public=dict(data['session'],media_directory=attempt,artifact_directory=attempt,processing={'task_id':task_id,'attempt_id':attempt})
        atomic(manifest,public)
        with store.tx() as db:
            current=db.execute('SELECT * FROM publications WHERE source_hash=?',(digest,)).fetchone()
            if current:
                db.execute("UPDATE tasks SET publication='superseded' WHERE attempt=?",(current['attempt_id'],))
                db.execute('UPDATE publications SET attempt_id=?,version=version+1 WHERE source_hash=?',(attempt,digest))
            else: db.execute('INSERT INTO publications VALUES(?,?,1)',(digest,attempt))
            db.execute("UPDATE tasks SET publication='active' WHERE id=?",(task_id,))
            db.execute("UPDATE publication_intents SET status='completed' WHERE task_id=?",(task_id,))
        return {'status':'active','attempt_id':attempt}
