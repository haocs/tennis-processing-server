"""Safe storage references and publication by same-filesystem rename (no media recopy)."""
import copy
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import uuid
from store import Conflict, encode, uid
from feedback import (
    AI_PARAMETERS,
    InvalidFeedback,
    MAX_ARTIFACT_BYTES,
    MAX_BYTES,
    MAX_HEURISTICS_BYTES,
    PARAMETERS,
    build_snapshot_artifact,
    canonical,
    normalize_task_parameters,
    validate_heuristics,
    validate_parameter_map,
    validate_snapshot,
    validate_snapshot_artifact,
    version_id,
)


def inside(root, path):
    if not isinstance(path,str) or not path or '\\' in path or ':' in path or path.startswith('/') or '..' in PurePosixPath(path).parts:
        raise ValueError('invalid_storage_path')
    root_raw=Path(root)
    candidate=root_raw/path
    for cur in (candidate,*candidate.parents):
        if cur==root_raw:
            break
        if cur.is_symlink():
            raise ValueError('invalid_storage_path')
    root_res=root_raw.resolve()
    dest=candidate.resolve()
    if not dest.is_relative_to(root_res) or dest==root_res:
        raise ValueError('invalid_storage_path')
    return dest


def read_bounded_bytes(path, max_bytes, too_large_code, error_cls=ValueError):
    p=Path(path)
    if p.is_symlink() or not p.is_file():
        raise error_cls('invalid_storage_path')
    st=p.stat()
    if st.st_size>max_bytes:
        raise error_cls(too_large_code)
    with p.open('rb') as stream:
        raw=stream.read(max_bytes+1)
    if len(raw)>max_bytes:
        raise error_cls(too_large_code)
    return raw


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
        if not isinstance(ref,dict) or 'storage_id' not in ref or 'path' not in ref:
            raise ValueError('invalid_storage_path')
        return inside(self.root(ref['storage_id']),ref['path'])

    def validate_result(self, spec, body, attempt):
        if spec['type']=='dryrun':
            if body.get('result_ref'): raise ValueError('dryrun_result_must_be_local')
            return
        ref=body['result_ref']
        expected=f"runs/{body['task_id']}/{attempt}/result.json"
        if ref['storage_id']!='processing' or ref['path']!=expected: raise ValueError('result_outside_attempt')
        path=self.resolve(ref)
        raw_result=read_bounded_bytes(path,8*1024*1024,'result_manifest_too_large')
        data=json.loads(raw_result.decode())
        if data['attempt_id']!=attempt or data['task_id']!=body['task_id'] or not re.fullmatch('[a-f0-9]{64}',data['source_sha256']): raise ValueError('invalid_result_identity')
        if not data.get('files'): raise ValueError('empty_result')
        file_map={}
        for item in data['files']:
            if 'pose-observations' in item.get('path',''):
                raise ValueError('dense_observations_not_allowed')
            asset=inside(path.parent,item['path'])
            if asset.stat().st_size!=item['bytes'] or not re.fullmatch('[a-f0-9]{64}',item['sha256']): raise ValueError('invalid_artifact')
            file_map[item['path']]=item
        public=data['session']
        clips=public.get('clips')
        if public.get('id')!=data['source_sha256'] or not isinstance(clips,list) or public.get('video',{}).get('file')!='Practice.mp4':
            raise ValueError('invalid_session')
        for art in public.get('artifacts',[]):
            if 'pose-observations' in str(art):
                raise ValueError('dense_observations_not_allowed')
        declared=set(file_map)
        if not clips:
            # Empty-clip candidate: accepted only for never-auto-published candidate/experiment runs with duration_seconds == 0.
            if spec.get('publication_policy','never')!='never':
                raise ValueError('invalid_session')
            if float(public.get('video',{}).get('duration_seconds',0) or 0)!=0.0:
                raise ValueError('invalid_session')
            required={'metadata/original-metadata.json'} | {'metadata/'+a for a in public.get('artifacts',[])}
        else:
            required={'media/Practice.mp4','media/poster.jpg','metadata/original-metadata.json'} | {'media/'+c['file'] for c in clips} | {'metadata/'+a for a in public.get('artifacts',[])}
        if not required.issubset(declared): raise ValueError('missing_result_artifact')
        if not public.get('source',{}).get('sha256')==data['source_sha256']: raise ValueError('source_hash_mismatch')
        if 'metadata/heuristic-diagnostics.json' in declared or 'heuristic-diagnostics.json' in public.get('artifacts',[]):
            if 'metadata/heuristic-diagnostics.json' not in declared:
                raise ValueError('missing_result_artifact')
            h_item=file_map['metadata/heuristic-diagnostics.json']
            h_path=inside(path.parent,'metadata/heuristic-diagnostics.json')
            raw_bytes=read_bounded_bytes(h_path,MAX_HEURISTICS_BYTES,'heuristic_diagnostics_too_large')
            if hashlib.sha256(raw_bytes).hexdigest()!=h_item['sha256']:
                raise ValueError('invalid_artifact')
            try:
                h_data=json.loads(raw_bytes.decode())
            except (UnicodeDecodeError,json.JSONDecodeError):
                raise ValueError('invalid_heuristic_diagnostics')
            validate_heuristics(
                h_data,
                len(raw_bytes),
                data['source_sha256'],
                attempt,
                task_id=body['task_id'],
                published_at=public.get('published_at'),
                duration_seconds=public.get('source',{}).get('duration_seconds'),
                session=public,
            )
        return data

    def stage_candidate(self, store, task_id):
        import fcntl
        with open(store.path+'.publisher.lock','a') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX)
            return self._stage_candidate(store,task_id)

    def _stage_candidate(self, store, task_id):
        with store.tx() as db:
            task=db.execute('SELECT * FROM tasks WHERE id=?',(task_id,)).fetchone()
            if not task: raise KeyError('task_not_found')
            if task['status']!='succeeded': raise Conflict('task_not_completed')
            spec=json.loads(task['body'])
            if spec['type']!='process': raise ValueError('only_process_can_stage')
            result=json.loads(task['result']);attempt=task['attempt']
            att_row=db.execute('SELECT * FROM attempts WHERE id=?',(attempt,)).fetchone()
            if not att_row: raise KeyError('attempt_not_found')
            if att_row['media_state'] in ('quarantined','evicted'):
                raise Conflict('media_already_evicted')
            path=self.resolve(result['result_ref']);data=json.loads(path.read_text());digest=data['source_sha256']
            lib=self.root('library')
            att_body=json.loads(att_row['body']) if att_row['body'] else {}
            fallback_pub=float(data['session'].get('published_at') if data['session'].get('published_at') is not None else (att_body.get('finished_at') or task['created'] or 0.0))
            intent_row=db.execute('SELECT body FROM staging_intents WHERE task_id=?',(task_id,)).fetchone()
            if intent_row:
                saved_intent=json.loads(intent_row['body'])
                fallback_pub=float(saved_intent.get('published_at',fallback_pub))
            intent={'task_id':task_id,'attempt_id':attempt,'source_hash':digest,'published_at':fallback_pub}
            db.execute("INSERT OR REPLACE INTO staging_intents VALUES(?,?,'pending')",(task_id,encode(intent)))
        media=lib/'media'/attempt;metadata=lib/'metadata/runs'/attempt
        media.parent.mkdir(parents=True,exist_ok=True);metadata.parent.mkdir(parents=True,exist_ok=True)
        source_media=path.parent/'media';source_meta=path.parent/'metadata'
        is_empty_clips=not bool(data['session'].get('clips'))
        if not media.exists():
            if source_media.exists():
                if source_media.stat().st_dev!=media.parent.stat().st_dev: raise ValueError('publication_requires_same_filesystem')
                os.rename(source_media,media)
            elif is_empty_clips:
                media.mkdir(parents=True,exist_ok=True)
            else:
                raise Conflict('media_already_evicted')
        elif not is_empty_clips and not (media/'Practice.mp4').exists():
            raise Conflict('media_already_evicted')
        if not metadata.exists():
            if not source_meta.exists():
                raise OSError('missing_attempt_metadata')
            os.rename(source_meta,metadata)
        public=dict(data['session'],media_directory=attempt,artifact_directory=attempt,processing={'task_id':task_id,'attempt_id':attempt})
        public.setdefault('published_at',fallback_pub)
        atomic(metadata/'session.json',public)
        candidate_manifest=lib/'metadata/candidates'/digest/f'{attempt}.json'
        atomic(candidate_manifest,public)
        with store.tx() as db:
            now=store.clock()
            is_active=bool(db.execute('SELECT 1 FROM publications WHERE source_hash=? AND attempt_id=?',(digest,attempt)).fetchone())
            target_state='available' if is_active else 'staged_candidate'
            cur_att=db.execute('SELECT body FROM attempts WHERE id=?',(attempt,)).fetchone()
            cur_body=json.loads(cur_att['body']) if (cur_att and cur_att['body']) else {}
            cur_body.setdefault('staged_at',now)
            db.execute('UPDATE attempts SET media_state=?,gc_eligible_since=NULL,body=? WHERE id=?',(target_state,encode(cur_body),attempt))
            db.execute("UPDATE staging_intents SET status='completed' WHERE task_id=?",(task_id,))
            store._recompute_attempt_media_state(db,attempt)
            updated=db.execute('SELECT media_state FROM attempts WHERE id=?',(attempt,)).fetchone()
            final_state=updated['media_state']
        vid=version_id(digest,public['published_at'])
        return {
            'status':final_state,
            'task_id':task_id,
            'attempt_id':attempt,
            'source_sha256':digest,
            'video_version_id':vid,
            'media_state':final_state,
            'candidate_manifest':f'metadata/candidates/{digest}/{attempt}.json',
            'manifest':public,
        }

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
            att_row=db.execute('SELECT * FROM attempts WHERE id=?',(attempt,)).fetchone()
            if att_row and att_row['media_state'] in ('quarantined','evicted'):
                raise Conflict('media_already_evicted')
            path=self.resolve(result['result_ref']); data=json.loads(path.read_text());digest=data['source_sha256']
            if not data.get('session',{}).get('clips'):
                raise ValueError('empty_clips_cannot_activate')
            current=db.execute('SELECT * FROM publications WHERE source_hash=?',(digest,)).fetchone()
            lib=self.root('library');manifest=lib/'metadata/sessions'/f'{digest}.json'
            active=current['attempt_id'] if current else ('legacy' if manifest.exists() else None)
            intent_row=db.execute("SELECT body FROM publication_intents WHERE task_id=? AND status='pending'",(task_id,)).fetchone()
            if current is None and manifest.exists() and intent_row:
                pending=json.loads(intent_row['body'])
                written=json.loads(manifest.read_text())
                if written.get('processing',{}).get('attempt_id')==attempt:
                    active=pending['expected_active']
            if active==attempt:
                now=store.clock()
                db.execute("UPDATE attempts SET media_state='available',gc_eligible_since=NULL WHERE id=?",(attempt,))
                db.execute('INSERT OR REPLACE INTO artifact_pins VALUES(?,?,?,?,?,?,?,?)',('attempt',attempt,digest,'serving_active','active_publication',now,None,encode({'task_id':task_id})))
                return {'status':'active','attempt_id':attempt,'media_state':'available'}
            if policy=='if_no_active_result' and active is not None:
                db.execute("UPDATE tasks SET publication='not_requested' WHERE id=?",(task_id,))
                return {'status':'not_requested','active_attempt':active}
            if active!=expected_active: raise Conflict('active_result_changed')
            att_body=json.loads(att_row['body']) if (att_row and att_row['body']) else {}
            fallback_pub=float(data['session'].get('published_at') if data['session'].get('published_at') is not None else (att_body.get('finished_at') or task['created'] or 0.0))
            stage_row=db.execute('SELECT body FROM staging_intents WHERE task_id=?',(task_id,)).fetchone()
            if stage_row:
                fallback_pub=float(json.loads(stage_row['body']).get('published_at',fallback_pub))
            if intent_row:
                fallback_pub=float(json.loads(intent_row['body']).get('published_at',fallback_pub))
            intent={'expected_active':expected_active,'policy':policy,'attempt_id':attempt,'source_hash':digest,'published_at':fallback_pub}
            db.execute("INSERT OR REPLACE INTO publication_intents VALUES(?,?,'pending')",(task_id,encode(intent)))
            db.execute("UPDATE tasks SET publication='pending' WHERE id=?",(task_id,))
        media=lib/'media'/attempt;metadata=lib/'metadata/runs'/attempt
        media.parent.mkdir(parents=True,exist_ok=True);metadata.parent.mkdir(parents=True,exist_ok=True)
        source_media=path.parent/'media';source_meta=path.parent/'metadata'
        is_empty_clips=not bool(data['session'].get('clips'))
        if not media.exists():
            if source_media.exists():
                if source_media.stat().st_dev!=media.parent.stat().st_dev: raise ValueError('publication_requires_same_filesystem')
                os.rename(source_media,media)
            elif is_empty_clips:
                media.mkdir(parents=True,exist_ok=True)
            else:
                raise Conflict('media_already_evicted')
        elif not is_empty_clips and not (media/'Practice.mp4').exists():
            raise Conflict('media_already_evicted')
        if not metadata.exists():
            if not source_meta.exists():
                raise OSError('missing_attempt_metadata')
            os.rename(source_meta,metadata)
        public=dict(data['session'],media_directory=attempt,artifact_directory=attempt,processing={'task_id':task_id,'attempt_id':attempt})
        public.setdefault('published_at',fallback_pub)
        atomic(metadata/'session.json',public)
        atomic(manifest,public)
        with store.tx() as db:
            now=store.clock()
            current=db.execute('SELECT * FROM publications WHERE source_hash=?',(digest,)).fetchone()
            if current:
                prev_attempt=current['attempt_id']
                if prev_attempt!=attempt:
                    db.execute("UPDATE tasks SET publication='superseded' WHERE attempt=?",(prev_attempt,))
                    db.execute("DELETE FROM artifact_pins WHERE target_type='attempt' AND target_id=? AND holder_kind='serving_active'",(prev_attempt,))
                    prev_row=db.execute('SELECT * FROM attempts WHERE id=?',(prev_attempt,)).fetchone()
                    if prev_row and prev_row['worker_id']!='legacy-import' and prev_row['media_state'] in ('available','staged_candidate','gc_eligible'):
                        prev_body=json.loads(prev_row['body']) if prev_row['body'] else {}
                        prev_body['superseded_at']=now
                        db.execute('UPDATE attempts SET body=? WHERE id=?',(encode(prev_body),prev_attempt))
                        rollback_ttl=(self.config.get('retention') or {}).get('rollback_ttl_seconds')
                        rollback_expires=(now+float(rollback_ttl)) if rollback_ttl is not None else None
                        db.execute('INSERT OR REPLACE INTO artifact_pins VALUES(?,?,?,?,?,?,?,?)',('attempt',prev_attempt,digest,'rollback_previous','serving_rollback',now,rollback_expires,encode({'superseded_by':attempt})))
                        store._recompute_attempt_media_state(db,prev_attempt)
                db.execute('UPDATE publications SET attempt_id=?,version=version+1 WHERE source_hash=?',(attempt,digest))
            else:
                db.execute('INSERT INTO publications VALUES(?,?,1)',(digest,attempt))
            db.execute("DELETE FROM artifact_pins WHERE target_type='attempt' AND target_id=? AND holder_kind='rollback_previous'",(attempt,))
            db.execute('INSERT OR REPLACE INTO artifact_pins VALUES(?,?,?,?,?,?,?,?)',('attempt',attempt,digest,'serving_active','active_publication',now,None,encode({'task_id':task_id})))
            db.execute("UPDATE attempts SET media_state='available',gc_eligible_since=NULL WHERE id=?",(attempt,))
            db.execute("UPDATE tasks SET publication='active' WHERE id=?",(task_id,))
            db.execute("UPDATE publication_intents SET status='completed' WHERE task_id=?",(task_id,))
            db.execute("UPDATE staging_intents SET status='completed' WHERE task_id=?",(task_id,))
        return {'status':'active','attempt_id':attempt,'media_state':'available','video_version_id':version_id(digest,public['published_at'])}

    def _resolve_frozen_manifest(self, db, task_row, att_row):
        attempt_id=att_row['id']
        task_id=task_row['id']
        result=json.loads(att_row['result']) if att_row['result'] else {}
        if result.get('legacy') or att_row['worker_id']=='legacy-import':
            return None, None, False
        ref=result.get('result_ref')
        if not ref:
            return None, None, False
        digest=result.get('source_sha256')
        lib=self.root('library')
        run_session=lib/'metadata/runs'/attempt_id/'session.json'
        cand_session=lib/'metadata/candidates'/digest/f'{attempt_id}.json' if digest else None
        active_session=lib/'metadata/sessions'/f'{digest}.json' if digest else None
        manifest=None
        for p in (run_session,cand_session):
            if p and p.exists():
                manifest=json.loads(p.read_text())
                break
        if manifest is None and active_session and active_session.exists():
            loaded=json.loads(active_session.read_text())
            if (loaded.get('processing') or {}).get('attempt_id')==attempt_id:
                manifest=loaded
        result_path=self.resolve(ref)
        if manifest is None and result_path.exists():
            data=json.loads(result_path.read_text())
            digest=data['source_sha256']
            att_body=json.loads(att_row['body']) if att_row['body'] else {}
            fallback_pub=float(data['session'].get('published_at') if data['session'].get('published_at') is not None else (att_body.get('finished_at') or task_row['created'] or 0.0))
            manifest=dict(data['session'],media_directory=attempt_id,artifact_directory=attempt_id,processing={'task_id':task_id,'attempt_id':attempt_id})
            manifest.setdefault('published_at',fallback_pub)
        if manifest is None:
            return None, None, False
        heur_lib=lib/'metadata/runs'/attempt_id/'heuristic-diagnostics.json'
        heur_stage=result_path.parent/'metadata/heuristic-diagnostics.json'
        has_heuristics=False
        heur_ref=None
        if heur_lib.exists():
            has_heuristics=True
            heur_ref={'storage_id':'library','path':f'metadata/runs/{attempt_id}/heuristic-diagnostics.json'}
        elif heur_stage.exists():
            has_heuristics=True
            heur_ref={'storage_id':'processing','path':f'runs/{task_id}/{attempt_id}/metadata/heuristic-diagnostics.json'}
        return manifest, heur_ref, has_heuristics

    def list_video_versions(self, store, source_sha256=None, source_id=None, include_non_playable=False):
        with store.tx() as db:
            store.expire_pins(db)
            if source_id is not None:
                srow=db.execute('SELECT * FROM sources WHERE id=?',(source_id,)).fetchone()
                if not srow:
                    raise KeyError('source_not_found')
                sbody=json.loads(srow['body'])
                if source_sha256 is None:
                    source_sha256=sbody.get('sha256')
                elif sbody.get('sha256') and sbody['sha256']!=source_sha256:
                    raise ValueError('source_hash_mismatch')
            if source_sha256 is not None and not re.fullmatch('[a-f0-9]{64}',str(source_sha256)):
                raise ValueError('invalid_source_digest')
            if not source_sha256:
                return []
            pub_row=db.execute('SELECT attempt_id FROM publications WHERE source_hash=?',(source_sha256,)).fetchone()
            active_attempt=pub_row['attempt_id'] if pub_row else None
            rows=db.execute("SELECT a.*,t.source_id,t.body AS task_body,t.publication AS task_publication,t.created AS task_created FROM attempts a JOIN tasks t ON a.task_id=t.id WHERE a.status='succeeded' AND json_extract(t.body,'$.type')='process' ORDER BY t.created DESC,a.number DESC").fetchall()
            lib=self.root('library')
            items=[]
            for row in rows:
                if source_id is not None and row['source_id']!=source_id:
                    continue
                res=json.loads(row['result']) if row['result'] else {}
                if res.get('source_sha256')!=source_sha256:
                    continue
                att_id=row['id']
                store._recompute_attempt_media_state(db,att_id)
                att_row=db.execute('SELECT * FROM attempts WHERE id=?',(att_id,)).fetchone()
                t_row=db.execute('SELECT * FROM tasks WHERE id=?',(row['task_id'],)).fetchone()
                manifest,heur_ref,has_heuristics=self._resolve_frozen_manifest(db,t_row,att_row)
                if not manifest:
                    continue
                has_clips=bool(manifest.get('clips'))
                mstate=att_row['media_state']
                if mstate in ('available','staged_candidate','gc_eligible'):
                    if has_clips and not (lib/'media'/att_id/'Practice.mp4').exists():
                        mstate='unknown'
                    elif not has_clips and not (lib/'metadata/runs'/att_id/'session.json').exists() and not (lib/'metadata/candidates'/source_sha256/f'{att_id}.json').exists():
                        mstate='unknown'
                verified_state=mstate in ('available','staged_candidate','gc_eligible')
                playable=verified_state and has_clips
                if not verified_state and not include_non_playable:
                    continue
                vid=version_id(source_sha256,manifest['published_at'])
                summary=store.pin_summary(db,att_id,att_row['gc_eligible_since'])
                is_active=(att_id==active_attempt)
                pub_status='active' if is_active else row['task_publication']
                spec=json.loads(row['task_body'])
                items.append({
                    'id':vid,
                    'video_version_id':vid,
                    'source_sha256':source_sha256,
                    'attempt_id':att_id,
                    'task_id':row['task_id'],
                    'source_id':row['source_id'],
                    'published_at':manifest['published_at'],
                    'manifest':manifest,
                    'session':manifest,
                    'media_state':mstate,
                    'publication_status':pub_status,
                    'is_active':is_active,
                    'playable':playable,
                    'clip_count':len(manifest.get('clips') or []),
                    'has_heuristics':has_heuristics,
                    'heuristics_ref':heur_ref,
                    'pin_summary':summary,
                    'parameters':spec.get('parameters',{}),
                    'parent_task_id':spec.get('parent_task_id'),
                })
            items.sort(key=lambda x:(1 if x['is_active'] else 0,float(x['published_at'] or 0.0)),reverse=True)
            return items

    def staged_candidates_summary(self, store):
        """Return per-source summary of staged (non-active) candidate versions."""
        lib=self.root('library')
        by_source={}
        with store.tx() as db:
            pubs={r['source_hash']:r['attempt_id'] for r in db.execute('SELECT source_hash, attempt_id FROM publications').fetchall()}
            rows=db.execute(
                "SELECT a.id AS attempt_id, a.task_id, a.body AS attempt_body, t.result AS task_result, t.created AS task_created, s.body AS source_body "
                "FROM attempts a JOIN tasks t ON t.id=a.task_id LEFT JOIN sources s ON s.id=t.source_id "
                "WHERE a.media_state='staged_candidate' ORDER BY a.rowid DESC"
            ).fetchall()
        for row in rows:
            res_data=json.loads(row['task_result']) if row['task_result'] else {}
            src_data=json.loads(row['source_body']) if row['source_body'] else {}
            digest=res_data.get('source_sha256') or src_data.get('sha256')
            if not isinstance(digest,str) or not digest:
                continue
            if pubs.get(digest)==row['attempt_id']:
                continue
            cand_path=lib/'metadata/candidates'/digest/f"{row['attempt_id']}.json"
            if not cand_path.is_file():
                continue
            att_body=json.loads(row['attempt_body']) if row['attempt_body'] else {}
            staged_at=float(att_body.get('staged_at') if att_body.get('staged_at') is not None else (att_body.get('finished_at') or row['task_created'] or 0.0))
            entry=by_source.get(digest)
            if entry is None:
                by_source[digest]={
                    'count':1,
                    'latest_attempt_id':row['attempt_id'],
                    'latest_task_id':row['task_id'],
                    'latest_staged_at':staged_at,
                }
            else:
                entry['count']+=1
                if staged_at>entry['latest_staged_at']:
                    entry['latest_staged_at']=staged_at
                    entry['latest_attempt_id']=row['attempt_id']
                    entry['latest_task_id']=row['task_id']
        return {'by_source':by_source}

    def get_attempt_heuristics(self, store, attempt_id):
        with store.tx() as db:
            att_row=db.execute('SELECT * FROM attempts WHERE id=?',(attempt_id,)).fetchone()
            if not att_row:
                raise KeyError('attempt_not_found')
            task_row=db.execute('SELECT * FROM tasks WHERE id=?',(att_row['task_id'],)).fetchone()
            manifest,heur_ref,has_heuristics=self._resolve_frozen_manifest(db,task_row,att_row)
            if not has_heuristics or not heur_ref:
                raise KeyError('heuristics_not_found')
            path=self.resolve(heur_ref)
            raw=read_bounded_bytes(path,MAX_HEURISTICS_BYTES,'heuristic_diagnostics_too_large')
            data=json.loads(raw.decode())
            res=json.loads(att_row['result']) if att_row['result'] else {}
            validate_heuristics(
                data,
                len(raw),
                res.get('source_sha256') or manifest.get('id'),
                attempt_id,
                task_id=task_row['id'] if task_row else None,
                published_at=manifest.get('published_at'),
                duration_seconds=manifest.get('source',{}).get('duration_seconds'),
                session=manifest,
            )
            return data

    def create_feedback_snapshot(self, store, payload, idempotency_key=None):
        if not isinstance(payload,dict):
            raise InvalidFeedback('invalid_feedback_snapshot')
        base_attempt_id=payload.get('base_attempt_id')
        source_sha256=payload.get('source_sha256')
        if not isinstance(base_attempt_id,str) or not base_attempt_id:
            raise InvalidFeedback('base_attempt_mismatch')
        if not isinstance(source_sha256,str) or not re.fullmatch('[a-f0-9]{64}',source_sha256):
            raise InvalidFeedback('source_hash_mismatch')
        with store.tx() as db:
            store.expire_pins(db)
            att_row=db.execute('SELECT * FROM attempts WHERE id=?',(base_attempt_id,)).fetchone()
            if not att_row or att_row['status']!='succeeded':
                raise InvalidFeedback('base_attempt_mismatch')
            if att_row['media_state'] in ('quarantined','evicted'):
                raise Conflict('media_already_evicted')
            if att_row['worker_id']=='legacy-import' or att_row['media_state']=='never_rendered':
                raise InvalidFeedback('base_attempt_not_verified')
            task_row=db.execute('SELECT * FROM tasks WHERE id=?',(att_row['task_id'],)).fetchone()
            if not task_row:
                raise InvalidFeedback('base_task_mismatch')
            spec=json.loads(task_row['body'])
            if spec.get('type')!='process':
                raise InvalidFeedback('base_task_not_process')
            if payload.get('base_task_id') is not None and payload.get('base_task_id')!=task_row['id']:
                raise InvalidFeedback('base_task_mismatch')
            src_row=db.execute('SELECT * FROM sources WHERE id=?',(task_row['source_id'],)).fetchone()
            if not src_row:
                raise InvalidFeedback('source_not_found')
            source_id=src_row['id']
            src_body=json.loads(src_row['body'])
            att_res=json.loads(att_row['result']) if att_row['result'] else {}
            if src_body.get('sha256')!=source_sha256 or att_res.get('source_sha256')!=source_sha256 or source_id==source_sha256:
                raise InvalidFeedback('source_hash_mismatch')
            if att_row['media_state'] not in ('available','staged_candidate','gc_eligible'):
                raise InvalidFeedback('base_attempt_not_playable')
            base_manifest,_,_=self._resolve_frozen_manifest(db,task_row,att_row)
            if not base_manifest:
                raise InvalidFeedback('base_manifest_missing')
            validated,raw_bytes=build_snapshot_artifact(payload,base_manifest,base_task_id=task_row['id'])
            snapshot_sha256=hashlib.sha256(raw_bytes).hexdigest()
            rel_path=f'snapshots/{source_sha256}/{snapshot_sha256}.json'
            snap_path=inside(self.root('processing'),rel_path)

            def _persist():
                if snap_path.exists():
                    existing_bytes=read_bounded_bytes(snap_path,MAX_ARTIFACT_BYTES,'feedback_snapshot_too_large',InvalidFeedback)
                    if existing_bytes!=raw_bytes:
                        raise Conflict('snapshot_hash_collision')
                else:
                    snap_path.parent.mkdir(parents=True,exist_ok=True)
                    temp=snap_path.with_name('.'+snap_path.name+'.'+uid())
                    temp.write_bytes(raw_bytes)
                    os.replace(temp,snap_path)
                storage_ref={'storage_id':'processing','path':rel_path,'sha256':snapshot_sha256,'bytes':len(raw_bytes)}
                existing=db.execute('SELECT * FROM feedback_snapshots WHERE snapshot_sha256=?',(snapshot_sha256,)).fetchone()
                now=store.clock()
                if existing:
                    snap_id=existing['id']
                    created_at=existing['created']
                else:
                    snap_id='fbs_'+snapshot_sha256[:32]
                    created_at=now
                    db.execute(
                        'INSERT INTO feedback_snapshots VALUES(?,?,?,?,?,?,?,?,?,?)',
                        (snap_id,source_id,source_sha256,task_row['id'],base_attempt_id,validated['base_video_version_id'],snapshot_sha256,encode(storage_ref),encode(validated),created_at),
                    )
                db.execute(
                    'INSERT OR REPLACE INTO artifact_pins VALUES(?,?,?,?,?,?,?,?)',
                    ('attempt',base_attempt_id,source_sha256,'feedback_snapshot',snap_id,now,None,encode({'snapshot_sha256':snapshot_sha256})),
                )
                store._recompute_attempt_media_state(db,base_attempt_id)
                return {
                    'id':snap_id,
                    'snapshot_id':snap_id,
                    'source_id':source_id,
                    'source_sha256':source_sha256,
                    'base_task_id':task_row['id'],
                    'base_attempt_id':base_attempt_id,
                    'base_video_version_id':validated['base_video_version_id'],
                    'snapshot_sha256':snapshot_sha256,
                    'snapshot_ref':storage_ref,
                    'cut_constraints_ref':storage_ref,
                    'storage_ref':storage_ref,
                    'required_capabilities':['cut-constraints-v1'],
                    'parameter_overrides':validated.get('parameter_overrides',{}),
                    'created_at':created_at,
                }

            if idempotency_key:
                return store.idempotent(db,'feedback_snapshots',idempotency_key,payload,_persist)
            return _persist()

    def get_feedback_snapshot(self, store, identity):
        with store.tx() as db:
            row=db.execute('SELECT * FROM feedback_snapshots WHERE id=? OR snapshot_sha256=?',(identity,identity)).fetchone()
            if not row:
                raise KeyError('feedback_snapshot_not_found')
            sref=json.loads(row['storage_ref'])
            snap_body=json.loads(row['body'])
            return {
                'id':row['id'],
                'snapshot_id':row['id'],
                'source_id':row['source_id'],
                'source_sha256':row['source_sha256'],
                'base_task_id':row['base_task_id'],
                'base_attempt_id':row['base_attempt_id'],
                'base_video_version_id':row['base_video_version_id'],
                'snapshot_sha256':row['snapshot_sha256'],
                'snapshot_ref':sref,
                'cut_constraints_ref':sref,
                'storage_ref':sref,
                'required_capabilities':['cut-constraints-v1'],
                'parameter_overrides':snap_body.get('parameter_overrides',{}),
                'snapshot':snap_body,
                'created_at':row['created'],
            }

    def _merge_task_parameters(self, inherited_params, caller_params, snap_overrides):
        """Merge inherited < caller < snapshot overrides with nested ai_tuning support."""
        base = copy.deepcopy(inherited_params)
        if caller_params.get('processing_strategy') == 'vision_only' or snap_overrides.get('processing_strategy') == 'vision_only':
            for ai_key in AI_PARAMETERS:
                base.pop(ai_key, None)
        merged = base
        for pk, pv in caller_params.items():
            if pk == 'ai_tuning' and isinstance(pv, dict) and isinstance(merged.get('ai_tuning'), dict):
                merged['ai_tuning'] = {**merged['ai_tuning'], **copy.deepcopy(pv)}
            else:
                merged[pk] = copy.deepcopy(pv)
        for pk, pv in snap_overrides.items():
            if pk == 'ai_tuning' and isinstance(pv, dict) and isinstance(merged.get('ai_tuning'), dict):
                merged['ai_tuning'] = {**merged['ai_tuning'], **copy.deepcopy(pv)}
            else:
                merged[pk] = copy.deepcopy(pv)
        return merged

    def prepare_task_spec(self, store, b, parent_task=None):
        if b.get('type') not in ('process','dryrun','experiment'):
            raise ValueError('invalid_task_type')
        if b.get('executor','process-v2')!='process-v2':
            raise ValueError('unsupported_executor')
        source_id=b.get('source_id')
        if not isinstance(source_id,str) or not source_id:
            raise ValueError('source_id_required')
        if 'parameters' in b and b['parameters'] is not None and not isinstance(b['parameters'],dict):
            raise ValueError('invalid_parameters')
        with store.tx() as db:
            src_row=db.execute('SELECT * FROM sources WHERE id=?',(source_id,)).fetchone()
            if not src_row:
                raise KeyError('source_not_found')
            src_body=json.loads(src_row['body'])
            caller_params=copy.deepcopy(b.get('parameters') or {})
            inherited_params=copy.deepcopy((parent_task.get('parameters') or {}) if isinstance(parent_task,dict) else {})
            snapshot_id=b.get('feedback_snapshot_id')
            if snapshot_id is not None and 'cut_constraints_ref' not in caller_params:
                inherited_params.pop('cut_constraints_ref',None)
            snap_row=None
            snap_overrides={}
            constraints_ref=caller_params.get('cut_constraints_ref') if 'cut_constraints_ref' in caller_params else inherited_params.get('cut_constraints_ref')
            if snapshot_id is not None:
                if not isinstance(snapshot_id,str) or not snapshot_id:
                    raise ValueError('invalid_feedback_snapshot_id')
                snap_row=db.execute('SELECT * FROM feedback_snapshots WHERE id=? OR snapshot_sha256=?',(snapshot_id,snapshot_id)).fetchone()
                if not snap_row:
                    raise KeyError('feedback_snapshot_not_found')
                if snap_row['source_id']!=source_id:
                    raise ValueError('snapshot_source_mismatch')
                if parent_task is not None and snap_row['source_id']!=parent_task['source_id']:
                    raise ValueError('snapshot_source_mismatch')
                snap_ref=json.loads(snap_row['storage_ref'])
                snap_body=json.loads(snap_row['body'])
                if 'cut_constraints_ref' in caller_params and caller_params['cut_constraints_ref'] is not None:
                    supplied=caller_params['cut_constraints_ref']
                    if not isinstance(supplied,dict) or supplied.get('path')!=snap_ref['path'] or (supplied.get('sha256') and supplied['sha256']!=snap_ref['sha256']):
                        raise Conflict('conflicting_cut_constraints_ref')
                constraints_ref=snap_ref
                snap_overrides=snap_body.get('parameter_overrides') or {}
            req_caps=[]
            if 'required_capabilities' in b and b['required_capabilities'] is not None:
                if not isinstance(b['required_capabilities'],list) or any(not isinstance(x,str) or not x for x in b['required_capabilities']):
                    raise ValueError('invalid_capabilities')
                req_caps=list(dict.fromkeys(b['required_capabilities']))
            req_storage=[src_body['storage_id']]
            if b.get('type')!='dryrun' and 'processing' not in req_storage:
                req_storage.append('processing')
            verified_constraints_ref=None
            if constraints_ref is not None:
                if not isinstance(constraints_ref,dict) or constraints_ref.get('storage_id')!='processing' or not isinstance(constraints_ref.get('path'),str):
                    raise ValueError('invalid_cut_constraints_ref')
                c_path=self.resolve(constraints_ref)
                if not c_path.is_file() or c_path.is_symlink():
                    raise ValueError('cut_constraints_not_found')
                c_bytes=read_bounded_bytes(c_path,MAX_ARTIFACT_BYTES,'cut_constraints_too_large')
                c_sha=hashlib.sha256(c_bytes).hexdigest()
                if constraints_ref.get('sha256') and constraints_ref['sha256']!=c_sha:
                    raise ValueError('cut_constraints_hash_mismatch')
                if constraints_ref.get('bytes') is not None and constraints_ref['bytes']!=len(c_bytes):
                    raise ValueError('cut_constraints_size_mismatch')
                try:
                    c_artifact=json.loads(c_bytes.decode())
                    validate_snapshot_artifact(c_artifact)
                except (UnicodeDecodeError,json.JSONDecodeError,InvalidFeedback):
                    raise ValueError('invalid_cut_constraints_ref')
                if snap_row is None:
                    snap_row=db.execute('SELECT * FROM feedback_snapshots WHERE snapshot_sha256=?',(c_sha,)).fetchone()
                if not snap_row:
                    raise ValueError('unregistered_cut_constraints_snapshot')
                if snap_row['source_id']!=source_id or (src_body.get('sha256') and snap_row['source_sha256']!=src_body['sha256']):
                    raise ValueError('snapshot_source_mismatch')
                expected_rel=f"snapshots/{snap_row['source_sha256']}/{c_sha}.json"
                if constraints_ref['path']!=expected_rel:
                    raise ValueError('invalid_cut_constraints_ref')
                if not snap_overrides:
                    snap_overrides=(json.loads(snap_row['body']) or {}).get('parameter_overrides') or {}
                verified_constraints_ref={
                    'storage_id':'processing',
                    'path':expected_rel,
                    'sha256':c_sha,
                    'bytes':len(c_bytes),
                }
                if 'cut-constraints-v1' not in req_caps:
                    req_caps.append('cut-constraints-v1')
                if 'processing' not in req_storage:
                    req_storage.append('processing')
            for pk,pv in snap_overrides.items():
                if pk=='ai_tuning':
                    if 'ai_tuning' in caller_params:
                        if not isinstance(caller_params['ai_tuning'],dict):
                            raise ValueError('invalid_ai_tuning')
                        if isinstance(pv,dict):
                            for sk,sv in pv.items():
                                if sk in caller_params['ai_tuning'] and caller_params['ai_tuning'][sk]!=sv:
                                    raise Conflict('parameter_override_mismatch')
                elif pk in caller_params and caller_params[pk]!=pv:
                    raise Conflict('parameter_override_mismatch')
            merged_params=self._merge_task_parameters(inherited_params,caller_params,snap_overrides)
            merged_params.pop('cut_constraints_ref',None)
            raw_params=normalize_task_parameters(merged_params,ValueError,'invalid_parameters')
            if verified_constraints_ref is not None:
                raw_params['cut_constraints_ref']=verified_constraints_ref
            if raw_params.get('processing_strategy')=='hybrid_ai_vision' and 'hybrid-ai-vision-v1' not in req_caps:
                req_caps.append('hybrid-ai-vision-v1')
            if b.get('type')=='experiment' and not raw_params:
                raise ValueError('experiment_requires_parameters')
            pub_policy=b.get('publication_policy','never')
            if pub_policy not in ('never','if_no_active_result'):
                raise ValueError('invalid_publication_policy')
            if (snapshot_id is not None or 'cut_constraints_ref' in raw_params) and pub_policy!='never':
                raise ValueError('constrained_rerun_requires_never_publication_policy')
            spec={
                'source_id':source_id,
                'type':b['type'],
                'executor':'process-v2',
                'parameters':raw_params,
                'priority':int(b.get('priority',0)),
                'max_attempts':min(10,max(1,int(b.get('max_attempts',3)))),
                'publication_policy':pub_policy,
            }
            if req_caps:
                spec['required_capabilities']=req_caps
            if req_storage:
                spec['required_storage_ids']=req_storage
            if b.get('parent_task_id'):
                spec['parent_task_id']=b['parent_task_id']
            if b.get('trigger'):
                spec['trigger']=b['trigger']
            if snapshot_id is not None and snap_row is not None:
                spec['feedback_snapshot_id']=snap_row['id']
            return spec

    def _has_any_symlink(self, root_dir):
        if os.path.islink(root_dir):
            return True
        if not os.path.isdir(root_dir):
            return False
        for cur_root, dirs, files in os.walk(root_dir, followlinks=False):
            for name in (*dirs, *files):
                if os.path.islink(os.path.join(cur_root, name)):
                    return True
        return False

    def _lexical_eviction_paths(self, attempt_id):
        if not isinstance(attempt_id,str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}',attempt_id):
            return None
        lib=self.root('library')
        media_root=lib/'media'
        trash_root=media_root/'.trash'
        runs_meta_root=lib/'metadata'/'runs'
        for p in (lib,media_root,lib/'metadata',runs_meta_root):
            if os.path.islink(p):
                return None
        trash_root.mkdir(parents=True,exist_ok=True)
        if os.path.islink(trash_root):
            return None
        orig=media_root/attempt_id
        trash=trash_root/attempt_id
        meta_dir=runs_meta_root/attempt_id
        session_file=meta_dir/'session.json'
        if any(os.path.islink(p) for p in (orig,trash,meta_dir,session_file)):
            return None
        if self._has_any_symlink(orig) or self._has_any_symlink(trash):
            return None
        return orig,trash,session_file

    def _verify_attempt_eviction_identity(self, db, att_row, session_file):
        att_id=att_row['id']
        if att_row['worker_id']=='legacy-import' or att_row['status']!='succeeded':
            return False
        if db.execute('SELECT 1 FROM publications WHERE attempt_id=?',(att_id,)).fetchone():
            return False
        task_row=db.execute('SELECT * FROM tasks WHERE id=?',(att_row['task_id'],)).fetchone()
        if not task_row:
            return False
        src_row=db.execute('SELECT * FROM sources WHERE id=?',(task_row['source_id'],)).fetchone()
        if not src_row:
            return False
        src_body=json.loads(src_row['body']) if src_row['body'] else {}
        att_res=json.loads(att_row['result']) if att_row['result'] else {}
        digest=att_res.get('source_sha256')
        if not isinstance(digest,str) or not re.fullmatch('[a-f0-9]{64}',digest):
            return False
        if src_body.get('sha256')!=digest:
            return False
        try:
            raw_sess=read_bounded_bytes(session_file,MAX_ARTIFACT_BYTES,'session_manifest_too_large')
            sess=json.loads(raw_sess.decode())
        except (OSError,ValueError,UnicodeDecodeError,json.JSONDecodeError):
            return False
        if not isinstance(sess,dict):
            return False
        proc=sess.get('processing') if isinstance(sess.get('processing'),dict) else {}
        if (
            sess.get('id')!=digest
            or (sess.get('source') or {}).get('sha256')!=digest
            or sess.get('media_directory')!=att_id
            or sess.get('artifact_directory')!=att_id
            or proc.get('attempt_id')!=att_id
            or proc.get('task_id')!=task_row['id']
        ):
            return False
        ref=att_res.get('result_ref')
        if not isinstance(ref,dict):
            return False
        try:
            rpath=self.resolve(ref)
            raw_res=read_bounded_bytes(rpath,8*1024*1024,'result_manifest_too_large')
            rdata=json.loads(raw_res.decode())
        except (OSError,ValueError,UnicodeDecodeError,json.JSONDecodeError):
            return False
        if rdata.get('attempt_id')!=att_id or rdata.get('task_id')!=task_row['id'] or rdata.get('source_sha256')!=digest:
            return False
        return True

    def recover_quarantined(self, store):
        import fcntl
        with open(store.path+'.publisher.lock','a') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX)
            return self._recover_quarantined(store)

    def _recover_quarantined(self, store):
        recovered=[];conflicts=[]
        with store.tx() as db:
            rows=db.execute("SELECT * FROM attempts WHERE media_state='quarantined'").fetchall()
            valid_rows=[]
            for row in rows:
                att_id=row['id']
                paths=self._lexical_eviction_paths(att_id)
                if not paths or not self._verify_attempt_eviction_identity(db,row,paths[2]):
                    conflicts.append(att_id)
                    continue
                valid_rows.append((row,paths))
        for row,(orig,trash,_) in valid_rows:
            att_id=row['id']
            if os.path.islink(orig) or os.path.islink(trash) or self._has_any_symlink(orig) or self._has_any_symlink(trash):
                conflicts.append(att_id)
                continue
            orig_exists=os.path.lexists(orig)
            trash_exists=os.path.lexists(trash)
            if orig_exists and trash_exists:
                # Ambiguous state: both original and trash exist; keep fenced in 'quarantined', never delete or mark evicted!
                conflicts.append(att_id)
                continue
            if orig_exists and not trash_exists:
                os.rename(orig,trash)
                shutil.rmtree(trash)
            elif trash_exists and not orig_exists:
                shutil.rmtree(trash)
            if os.path.lexists(orig) or os.path.lexists(trash):
                conflicts.append(att_id)
                continue
            with store.tx() as db:
                db.execute("UPDATE attempts SET media_state='evicted',media_evicted_at=? WHERE id=? AND media_state='quarantined'",(store.clock(),att_id))
            recovered.append(att_id)
        return {'recovered':recovered,'conflicts':conflicts}

    def run_retention(self, store):
        import fcntl
        with open(store.path+'.publisher.lock','a') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX)
            recovery=self._recover_quarantined(store)
            retention_cfg=self.config.get('retention') or {}
            if not retention_cfg.get('enabled',False):
                return {'enabled':False,'evicted':[],'quarantined':[],'recovered':recovery['recovered'],'conflicts':recovery['conflicts']}
            grace=float(retention_cfg.get('grace_period_seconds',86400))
            with store.tx() as db:
                store.expire_pins(db,retention_enabled=True)
                candidates=db.execute("SELECT id FROM attempts WHERE media_state IN ('staged_candidate','available','gc_eligible')").fetchall()
                for c in candidates:
                    store._recompute_attempt_media_state(db,c['id'],retention_enabled=True)
                now=store.clock()
                eligible=db.execute("SELECT * FROM attempts WHERE media_state='gc_eligible' AND gc_eligible_since IS NOT NULL AND ?-gc_eligible_since>=?",(now,grace)).fetchall()
            evicted=[]
            for row in eligible:
                att_id=row['id']
                paths=self._lexical_eviction_paths(att_id)
                if not paths:
                    recovery['conflicts'].append(att_id)
                    continue
                orig,trash,session_file=paths
                quarantined_ok=False
                with store.tx() as db:
                    store.expire_pins(db,retention_enabled=True)
                    now=store.clock()
                    pin_count=db.execute("SELECT COUNT(*) FROM artifact_pins WHERE target_type='attempt' AND target_id=? AND (expires IS NULL OR expires>?)",(att_id,now)).fetchone()[0]
                    is_active=bool(db.execute('SELECT 1 FROM publications WHERE attempt_id=?',(att_id,)).fetchone())
                    cur=db.execute('SELECT * FROM attempts WHERE id=?',(att_id,)).fetchone()
                    if pin_count>0 or is_active or not cur or cur['media_state']!='gc_eligible':
                        continue
                    if not self._verify_attempt_eviction_identity(db,cur,session_file):
                        recovery['conflicts'].append(att_id)
                        continue
                    if os.path.lexists(orig) and os.path.lexists(trash):
                        recovery['conflicts'].append(att_id)
                        continue
                    db.execute("UPDATE attempts SET media_state='quarantined' WHERE id=?",(att_id,))
                    quarantined_ok=True
                if not quarantined_ok:
                    continue
                if os.path.islink(orig) or os.path.islink(trash) or (os.path.lexists(orig) and os.path.lexists(trash)):
                    recovery['conflicts'].append(att_id)
                    continue
                if os.path.lexists(orig):
                    os.rename(orig,trash)
                if os.path.lexists(trash):
                    shutil.rmtree(trash)
                if os.path.lexists(orig) or os.path.lexists(trash):
                    recovery['conflicts'].append(att_id)
                    continue
                with store.tx() as db:
                    db.execute("UPDATE attempts SET media_state='evicted',media_evicted_at=? WHERE id=?",(store.clock(),att_id))
                evicted.append(att_id)
            return {'enabled':True,'evicted':evicted,'recovered':recovery['recovered'],'conflicts':recovery['conflicts']}
