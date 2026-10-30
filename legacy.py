"""Import existing published recordings without reprocessing or invented timings."""
import json
from pathlib import Path
from store import encode, uid


def import_library(store,files):
    originals=files.root('originals');library=files.root('library');by_name={}
    for p in originals.rglob('*'):
        if p.is_file() and not p.is_symlink() and p.suffix.lower() in ('.mov','.mp4','.m4v'):
            by_name.setdefault(p.name.casefold(),[]).append(p)
    imported=0;unmatched=[]
    for path in (library/'metadata/sessions').glob('*.json'):
        data=json.loads(path.read_text());digest=data['id'];name=data['source']['name']
        matches=by_name.get(name.casefold(),[])
        if len(matches)!=1:
            unmatched.append(name);continue
        p=matches[0];stat=p.stat();source=store.source({'storage_id':'originals','path':p.relative_to(originals).as_posix(),'bytes':stat.st_size,'modified_ns':stat.st_mtime_ns,'sha256':digest})
        with store.tx() as db:
            if db.execute("SELECT 1 FROM tasks WHERE source_id=? AND json_extract(body,'$.trigger')='legacy_import'",(source,)).fetchone():continue
            spec={'source_id':source,'type':'process','executor':'legacy','parameters':{},'priority':0,'max_attempts':1,'publication_policy':'never','trigger':'legacy_import','algorithm':data.get('algorithm'),'timing_unknown':True}
            task=store._enqueue(db,spec)['task_id'];attempt=uid()
            result={'legacy':True,'source_sha256':digest,'result_ref':{'storage_id':'library','path':path.relative_to(library).as_posix()},'timing':{'total_seconds':None,'stages':[],'measurement':'Historical processing timings unknown; imported publication only.'}}
            db.execute('INSERT INTO attempts VALUES(?,?,?,?,?,?,?,?,?)',(attempt,task,'legacy-import','',0,'succeeded',1,encode({'imported_at':store.clock()}),encode(result)))
            db.execute("UPDATE tasks SET status='succeeded',attempt=?,result=?,publication='active' WHERE id=?",(attempt,encode(result),task))
            db.execute('INSERT OR IGNORE INTO publications VALUES(?,?,1)',(digest,attempt));imported+=1
    return {'imported':imported,'unmatched_sources':unmatched}
