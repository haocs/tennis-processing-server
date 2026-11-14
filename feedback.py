"""Portable, immutable feedback commands. Evidence is never executable by itself."""
import copy
import hashlib
import json
import math
import re

MAX_BYTES=128*1024
MAX_ARTIFACT_BYTES=256*1024
MAX_HEURISTICS_BYTES=512*1024
COMMANDS={'keep_intervals_ms','exclude_intervals_ms','override_clip_bounds_ms'}
NUMERIC_PARAMETERS={'target_region','min_player_height','swing_speed','arm_reach','padding_before','padding_after','merge_gap'}
AI_PARAMETERS={'ai_model','allow_model_fallback','ai_tuning'}
PARAMETERS=NUMERIC_PARAMETERS|{'processing_strategy'}|AI_PARAMETERS
PROCESSING_STRATEGIES=('vision_only','hybrid_ai_vision','ai_only')
SUPPORTED_PROCESSING_STRATEGIES=('vision_only','hybrid_ai_vision')
AI_AGGRESSIVENESS_LEVELS=('conservative','balanced','aggressive')
AI_ACCURACY_TIERS=('fast','standard','thorough')
AI_PROVIDERS=('antigravity','gemini','codex')
AI_MODEL_RE=re.compile(r'^(antigravity|gemini|codex):[A-Za-z0-9._-]{1,64}$')
FORBIDDEN_TRUTH_CLAIMS={'verified_play','verified_tennis','verified_swing','ground_truth','human_verified','benchmark_truth'}

class InvalidFeedback(ValueError):
    pass


def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()


def version_id(source,publication):
    return 'v_'+hashlib.sha256(json.dumps([source,float(publication)],separators=(',',':'),allow_nan=False).encode()).hexdigest()[:32]


def overlaps(a,b):
    return max(a[0],b[0])<min(a[1],b[1])


def _validate_ai_tuning(tuning,error_cls):
    if not isinstance(tuning,dict) or set(tuning)-{'aggressiveness','accuracy_tier'}:
        raise error_cls('invalid_ai_tuning')
    if 'aggressiveness' in tuning:
        agg=tuning['aggressiveness']
        if not isinstance(agg,str) or agg not in AI_AGGRESSIVENESS_LEVELS:
            raise error_cls('invalid_ai_tuning')
    if 'accuracy_tier' in tuning:
        tier=tuning['accuracy_tier']
        if not isinstance(tier,str) or tier not in AI_ACCURACY_TIERS:
            raise error_cls('invalid_ai_tuning')


def validate_parameter_map(parameters,error_cls=InvalidFeedback,code='invalid_parameter_overrides'):
    """Validate task or snapshot parameter overrides without mutating the input mapping."""
    def fail():raise error_cls(code)
    if not isinstance(parameters,dict) or set(parameters)-PARAMETERS:fail()
    strategy='vision_only'
    if 'processing_strategy' in parameters:
        strategy=parameters['processing_strategy']
        if not isinstance(strategy,str) or strategy not in PROCESSING_STRATEGIES:
            raise error_cls('invalid_processing_strategy')
        if strategy=='ai_only':
            raise error_cls('unsupported_processing_strategy')
    if strategy=='vision_only' and any(k in parameters for k in AI_PARAMETERS):
        raise error_cls('ai_parameters_require_ai_strategy')
    for key,value in parameters.items():
        if key=='target_region':
            if not isinstance(value,list) or len(value)!=4 or any(type(v) not in (float,int) or not math.isfinite(v) or not 0<=v<=1 for v in value) or not value[0]<value[2] or not value[1]<value[3]:fail()
        elif key=='processing_strategy':
            continue
        elif key=='ai_model':
            if not isinstance(value,str) or (value!='auto' and not AI_MODEL_RE.fullmatch(value)):
                raise error_cls('invalid_ai_model')
        elif key=='allow_model_fallback':
            if type(value) is not bool:
                raise error_cls('invalid_allow_model_fallback')
        elif key=='ai_tuning':
            _validate_ai_tuning(value,error_cls)
        elif type(value) not in (int,float) or not math.isfinite(value) or value<0 or value>10000:fail()


def normalize_task_parameters(parameters,error_cls=ValueError,code='invalid_parameters'):
    """Validate and return a normalized parameter dict preserving legacy empty/{} omissions."""
    validate_parameter_map(parameters,error_cls=error_cls,code=code)
    out=copy.deepcopy(parameters)
    if out.get('processing_strategy')=='hybrid_ai_vision':
        out.setdefault('ai_model','auto')
        if 'allow_model_fallback' not in out:
            out['allow_model_fallback']=True
        tuning=dict(out.get('ai_tuning') or {})
        out['ai_tuning']={
            'aggressiveness':tuning.get('aggressiveness','balanced'),
            'accuracy_tier':tuning.get('accuracy_tier','standard'),
        }
    return out


def validate_snapshot(payload,base):
    """Validate against the verified frozen base manifest, not caller-supplied bounds."""
    def fail(message):raise InvalidFeedback(message)
    if not isinstance(payload,dict):fail('invalid_feedback_snapshot')
    allowed={'schema_version','source_sha256','base_task_id','base_attempt_id','base_video_version_id','selected_evidence_revisions','confirmed_commands','parameter_overrides','compiler_version'}
    if set(payload)-allowed or type(payload.get('schema_version',1)) is not int or payload.get('schema_version',1)!=1:fail('invalid_feedback_snapshot')
    try:raw=canonical(payload)
    except (ValueError,TypeError):fail('invalid_feedback_json')
    if len(raw)>MAX_BYTES:fail('feedback_snapshot_too_large')
    source=base.get('id')
    if not isinstance(source,str) or not re.fullmatch('[a-f0-9]{64}',source) or payload.get('source_sha256')!=source:fail('source_hash_mismatch')
    processing=base.get('processing') or {}
    attempt=processing.get('attempt_id')
    if not attempt or payload.get('base_attempt_id')!=attempt:fail('base_attempt_mismatch')
    if payload.get('base_task_id') is not None and payload.get('base_task_id')!=processing.get('task_id'):fail('base_task_mismatch')
    published_at=base.get('published_at')
    if type(published_at) not in (int,float) or not math.isfinite(published_at):fail('invalid_base_publication')
    if payload.get('base_video_version_id')!=version_id(source,published_at):fail('base_version_mismatch')
    if payload.get('compiler_version')!='cut-constraints-compiler-v1':fail('unsupported_compiler')
    duration=base.get('source',{}).get('duration_seconds')
    if type(duration) not in (int,float) or not math.isfinite(duration) or duration<=0:fail('invalid_source_duration')
    duration_ms=round(duration*1000)
    evidence=payload.get('selected_evidence_revisions')
    if not isinstance(evidence,list) or not 1<=len(evidence)<=50:fail('invalid_selected_evidence')
    refs=set()
    clips={c['id']:c for c in base.get('clips',[]) if isinstance(c,dict) and 'id' in c}
    for item in evidence:
        if not isinstance(item,dict) or set(item)-{'annotation_id','version','created_at','updated_at','kind','target','body','context'}:fail('invalid_evidence_revision')
        identity=item.get('annotation_id');revision=item.get('version')
        if not isinstance(identity,str) or not 1<=len(identity)<=100 or type(revision) is not int or revision<1 or (identity,revision) in refs:fail('invalid_evidence_revision')
        if item.get('kind')!='cut_feedback' or not isinstance(item.get('body'),dict) or not isinstance(item.get('target'),dict):fail('invalid_evidence_revision')
        context=item.get('context',{})
        if not isinstance(context,dict) or context.get('publication')!=published_at:fail('evidence_version_mismatch')
        target=item['target'];scope=target.get('scope')
        if scope=='clip':
            clip=clips.get(target.get('clip_id'))
            if not clip or target.get('start_seconds')!=clip['original_start_seconds'] or target.get('end_seconds')!=clip['original_end_seconds']:fail('evidence_target_mismatch')
        elif scope=='interval':
            start,end=target.get('start_seconds'),target.get('end_seconds')
            if any(type(v) not in (int,float) or not math.isfinite(v) for v in (start,end)) or not 0<=start<end<=duration:fail('evidence_target_mismatch')
            if any(overlaps((start,end),(c['original_start_seconds'],c['original_end_seconds'])) for c in clips.values()):fail('evidence_not_in_removed_gap')
        else:fail('invalid_evidence_target')
        refs.add((identity,revision))
    commands=payload.get('confirmed_commands')
    if not isinstance(commands,dict) or set(commands)!=COMMANDS:fail('invalid_commands')
    positive=[];negative=[];overrides=[];seen_clips=set();count=0
    for kind,rows in commands.items():
        if not isinstance(rows,list) or len(rows)>50:fail('invalid_commands')
        for row in rows:
            if not isinstance(row,dict):fail('invalid_command')
            override=kind=='override_clip_bounds_ms'
            keys={'clip_id','original_start_ms','original_end_ms'} if override else {'start_ms','end_ms'}
            if set(row)!=(keys|{'source_annotation_id','source_revision'}):fail('invalid_command_fields')
            if (row['source_annotation_id'],row['source_revision']) not in refs or type(row['source_revision']) is not int:fail('command_evidence_missing')
            start,end=(row['original_start_ms'],row['original_end_ms']) if override else (row['start_ms'],row['end_ms'])
            if type(start) is not int or type(end) is not int or not 0<=start<end<=duration_ms:fail('invalid_command_interval')
            interval=(start,end)
            if override:
                if row['clip_id'] not in clips:fail('override_clip_not_in_base')
                base_clip=clips[row['clip_id']]
                base_span=(round(base_clip['original_start_seconds']*1000),round(base_clip['original_end_seconds']*1000))
                if not overlaps(interval,base_span) or any(overlaps(interval,(round(c['original_start_seconds']*1000),round(c['original_end_seconds']*1000))) for key,c in clips.items() if key!=row['clip_id']):fail('override_crosses_base_clip')
                if row['clip_id'] in seen_clips or any(overlaps(interval,old) for old in overrides):fail('contradictory_constraints')
                seen_clips.add(row['clip_id']);overrides.append(interval)
            (negative if kind=='exclude_intervals_ms' else positive).append(interval);count+=1
    if not count:fail('no_confirmed_commands')
    if any(overlaps(a,b) for a in positive for b in negative):fail('contradictory_constraints')
    parameters=payload.get('parameter_overrides',{})
    validate_parameter_map(parameters,InvalidFeedback,'invalid_parameter_overrides')
    result=copy.deepcopy(payload)
    result['schema_version']=1
    return result


def build_snapshot_artifact(payload,base,base_task_id=None):
    """Validate inbound snapshot (<=128KiB, no client base_manifest) and embed verified server base_manifest (<=256KiB)."""
    if isinstance(payload,dict) and 'base_manifest' in payload:
        raise InvalidFeedback('client_base_manifest_forbidden')
    validated=validate_snapshot(payload,base)
    if base_task_id is not None:
        validated['base_task_id']=base_task_id
    elif (base.get('processing') or {}).get('task_id'):
        validated['base_task_id']=base['processing']['task_id']
    validated['base_manifest']=copy.deepcopy(base)
    raw=canonical(validated)
    if len(raw)>MAX_ARTIFACT_BYTES:
        raise InvalidFeedback('feedback_snapshot_too_large')
    return validated,raw


def validate_snapshot_artifact(artifact):
    """Verify an enriched on-disk snapshot artifact containing server-verified base_manifest."""
    def fail(message):raise InvalidFeedback(message)
    if not isinstance(artifact,dict) or 'base_manifest' not in artifact:
        fail('missing_base_manifest')
    try:
        raw=canonical(artifact)
    except (ValueError,TypeError):
        fail('invalid_feedback_json')
    if len(raw)>MAX_ARTIFACT_BYTES:
        fail('feedback_snapshot_too_large')
    base=artifact.get('base_manifest')
    if not isinstance(base,dict):
        fail('invalid_base_manifest')
    inbound={k:v for k,v in artifact.items() if k!='base_manifest'}
    validate_snapshot(inbound,base)
    return artifact


def _contains_forbidden_truth(obj):
    if isinstance(obj,dict):
        for k,v in obj.items():
            if k in FORBIDDEN_TRUTH_CLAIMS and v is not False:
                return True
            if k in ('verified_play_or_swing',) and v is not False:
                return True
            if _contains_forbidden_truth(v):
                return True
    elif isinstance(obj,list):
        return any(_contains_forbidden_truth(item) for item in obj)
    elif isinstance(obj,str):
        return obj in FORBIDDEN_TRUTH_CLAIMS
    return False


def validate_heuristics(data,raw_size,source_sha256,attempt_id,task_id=None,published_at=None,duration_seconds=None,session=None):
    """Independently verify worker heuristic-diagnostics.json <= 512 KiB and contract invariants."""
    if raw_size>MAX_HEURISTICS_BYTES:
        raise ValueError('heuristic_diagnostics_too_large')
    if not isinstance(data,dict) or type(data.get('schema_version')) is not int or data.get('schema_version')!=1:
        raise ValueError('invalid_heuristic_diagnostics')
    try:
        raw_canonical=canonical(data)
    except (ValueError,TypeError):
        raise ValueError('invalid_heuristic_diagnostics')
    if len(raw_canonical)>MAX_HEURISTICS_BYTES:
        raise ValueError('heuristic_diagnostics_too_large')
    if data.get('truth_status')!='heuristic_only_not_play_or_swing_truth':
        raise ValueError('invalid_heuristic_truth_status')
    if _contains_forbidden_truth(data):
        raise ValueError('forbidden_heuristic_truth_claim')
    if data.get('source_sha256')!=source_sha256:
        raise ValueError('heuristic_source_mismatch')
    cache_key=data.get('extraction_cache_key')
    provenance=data.get('provenance')
    if cache_key is not None:
        if not isinstance(cache_key,str) or not re.fullmatch('[a-f0-9]{64}',cache_key):
            raise ValueError('invalid_heuristic_provenance')
    if provenance is not None:
        if not isinstance(provenance,dict) or provenance.get('source_sha256') not in (None,source_sha256):
            raise ValueError('invalid_heuristic_provenance')
        if cache_key is not None and provenance.get('extraction_cache_key') not in (None,cache_key):
            raise ValueError('invalid_heuristic_provenance')
    pub_obj=data.get('publication') if isinstance(data.get('publication'),dict) else {}
    att=data.get('attempt_id') if data.get('attempt_id') is not None else pub_obj.get('attempt_id')
    if att is not None and att!=attempt_id:
        raise ValueError('heuristic_attempt_mismatch')
    t_id=data.get('task_id') if data.get('task_id') is not None else pub_obj.get('task_id')
    if task_id is not None and t_id not in (None,task_id):
        raise ValueError('heuristic_task_mismatch')
    if published_at is not None:
        diag_pub=data.get('published_at') if data.get('published_at') is not None else pub_obj.get('published_at')
        if diag_pub is not None and (type(diag_pub) not in (int,float) or abs(float(diag_pub)-float(published_at))>1e-6):
            raise ValueError('heuristic_publication_mismatch')
        diag_vid=data.get('video_version_id') if data.get('video_version_id') is not None else pub_obj.get('video_version_id')
        if diag_vid is not None and diag_vid!=version_id(source_sha256,published_at):
            raise ValueError('heuristic_version_mismatch')
    clips_list=data.get('clips') if 'clips' in data else data.get('kept_clips')
    gaps_list=data.get('removed_gaps')
    if not isinstance(clips_list,list) or not isinstance(gaps_list,list):
        raise ValueError('invalid_heuristic_intervals')
    dur_ms=data.get('source_duration_ms')
    if dur_ms is not None:
        if type(dur_ms) is not int or dur_ms<=0:
            raise ValueError('invalid_heuristic_intervals')
        if duration_seconds is not None and type(duration_seconds) in (int,float) and abs(dur_ms-round(float(duration_seconds)*1000))>5:
            raise ValueError('invalid_heuristic_intervals')
    parsed_clip_intervals=[]
    for item in clips_list:
        if not isinstance(item,dict):
            raise ValueError('invalid_heuristic_intervals')
        if 'original_start_ms' in item or 'original_end_ms' in item:
            s_ms,e_ms=item.get('original_start_ms'),item.get('original_end_ms')
            if type(s_ms) is not int or type(e_ms) is not int or s_ms<0 or e_ms<=s_ms:
                raise ValueError('invalid_heuristic_intervals')
            if dur_ms is not None and e_ms>dur_ms:
                raise ValueError('invalid_heuristic_intervals')
        else:
            s,e=item.get('original_start_seconds'),item.get('original_end_seconds')
            if any(type(v) not in (int,float) or not math.isfinite(v) for v in (s,e)) or s<0 or e<=s:
                raise ValueError('invalid_heuristic_intervals')
            if duration_seconds is not None and type(duration_seconds) in (int,float) and e>duration_seconds+1e-3:
                raise ValueError('invalid_heuristic_intervals')
            s_ms,e_ms=round(float(s)*1000),round(float(e)*1000)
        for hint in (item.get('hints') or []):
            if not isinstance(hint,dict):
                raise ValueError('invalid_heuristic_intervals')
            if 'start_ms' in hint or 'end_ms' in hint:
                hs,he=hint.get('start_ms'),hint.get('end_ms')
                if type(hs) is not int or type(he) is not int or hs<s_ms or he>e_ms or he<=hs:
                    raise ValueError('invalid_heuristic_intervals')
        parsed_clip_intervals.append((s_ms,e_ms,item))
    for item in gaps_list:
        if not isinstance(item,dict):
            raise ValueError('invalid_heuristic_intervals')
        if 'original_start_ms' in item or 'original_end_ms' in item:
            s_ms,e_ms=item.get('original_start_ms'),item.get('original_end_ms')
            if type(s_ms) is not int or type(e_ms) is not int or s_ms<0 or e_ms<=s_ms:
                raise ValueError('invalid_heuristic_intervals')
            if dur_ms is not None and e_ms>dur_ms:
                raise ValueError('invalid_heuristic_intervals')
        else:
            s,e=item.get('original_start_seconds'),item.get('original_end_seconds')
            if any(type(v) not in (int,float) or not math.isfinite(v) for v in (s,e)) or s<0 or e<=s:
                raise ValueError('invalid_heuristic_intervals')
            if duration_seconds is not None and type(duration_seconds) in (int,float) and e>duration_seconds+1e-3:
                raise ValueError('invalid_heuristic_intervals')
            s_ms,e_ms=round(float(s)*1000),round(float(e)*1000)
        if any(overlaps((s_ms,e_ms),(cs,ce)) for cs,ce,_ in parsed_clip_intervals):
            raise ValueError('invalid_heuristic_intervals')
        for hint in (item.get('hints') or []):
            if not isinstance(hint,dict):
                raise ValueError('invalid_heuristic_intervals')
            if 'start_ms' in hint or 'end_ms' in hint:
                hs,he=hint.get('start_ms'),hint.get('end_ms')
                if type(hs) is not int or type(he) is not int or hs<s_ms or he>e_ms or he<=hs:
                    raise ValueError('invalid_heuristic_intervals')
    if isinstance(session,dict) and isinstance(session.get('clips'),list):
        sess_clips=session['clips']
        if len(sess_clips)!=len(parsed_clip_intervals):
            raise ValueError('heuristic_clip_count_mismatch')
        for (s_ms,e_ms,d_clip),s_clip in zip(parsed_clip_intervals,sess_clips):
            if not isinstance(s_clip,dict):
                continue
            if s_clip.get('id') and d_clip.get('clip_id') and d_clip['clip_id']!=s_clip['id']:
                raise ValueError('heuristic_clip_id_mismatch')
            if 'original_start_seconds' in s_clip and 'original_end_seconds' in s_clip:
                exp_s=round(float(s_clip['original_start_seconds'])*1000)
                exp_e=round(float(s_clip['original_end_seconds'])*1000)
                if abs(s_ms-exp_s)>2 or abs(e_ms-exp_e)>2:
                    raise ValueError('heuristic_clip_bounds_mismatch')
    return data
