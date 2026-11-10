"""Import existing published recordings without reprocessing or invented timings."""
import json
from pathlib import Path
from store import encode, uid
from files import atomic


def import_library(store,files):
    originals=files.root('originals');library=files.root('library');by_name={}
    for p in originals.rglob('*'):
        if p.is_file() and not p.is_symlink() and p.suffix.lower() in ('.mov','.mp4','.m4v'):
            by_name.setdefault(p.name.casefold(),[]).append(p)
    imported=0;unmatched=[]
    for path in (library/'metadata/sessions').glob('*.json'):
        data=json.loads(path.read_text());digest=data['id'];name=data['source']['name']
        archive=files.root('processing')/'legacy'/digest/'session.json'
        if not archive.exists():
            if data.get('processing'): continue  # Already a native task, not legacy history.
            atomic(archive,data)
        data=json.loads(archive.read_text())
        if data['id']!=digest: raise ValueError('legacy_archive_identity_mismatch')
        matches=by_name.get(name.casefold(),[])
        if len(matches)!=1:
            unmatched.append(name);continue
        p=matches[0];stat=p.stat();source=store.source({'storage_id':'originals','path':p.relative_to(originals).as_posix(),'bytes':stat.st_size,'modified_ns':stat.st_mtime_ns,'sha256':digest})
        with store.tx() as db:
            reference={'storage_id':'processing','path':archive.relative_to(files.root('processing')).as_posix()}
            existing=db.execute("SELECT * FROM tasks WHERE source_id=? AND json_extract(body,'$.trigger')='legacy_import'",(source,)).fetchone()
            if existing:
                result=json.loads(existing['result']);result['result_ref']=reference
                db.execute('UPDATE tasks SET result=? WHERE id=?',(encode(result),existing['id']))
                db.execute('UPDATE attempts SET result=? WHERE id=?',(encode(result),existing['attempt']))
                continue
            spec={'source_id':source,'type':'process','executor':'legacy','parameters':{},'priority':0,'max_attempts':1,'publication_policy':'never','trigger':'legacy_import','algorithm':data.get('algorithm'),'timing_unknown':True}
            task=store._enqueue(db,spec)['task_id'];attempt=uid()
            result={'legacy':True,'source_sha256':digest,'result_ref':reference,'timing':{'total_seconds':None,'stages':[],'measurement':'Historical processing timings unknown; imported publication only.'}}
            db.execute("INSERT INTO attempts(id,task_id,worker_id,token,expires,status,number,body,result,media_state) VALUES(?,?,?,?,?,?,?,?,?,'unknown')",(attempt,task,'legacy-import','',0,'succeeded',1,encode({'imported_at':store.clock()}),encode(result)))
            db.execute("UPDATE tasks SET status='succeeded',attempt=?,result=?,publication='active' WHERE id=?",(attempt,encode(result),task))
            db.execute('INSERT OR IGNORE INTO publications VALUES(?,?,1)',(digest,attempt));imported+=1
    return {'imported':imported,'unmatched_sources':unmatched}
