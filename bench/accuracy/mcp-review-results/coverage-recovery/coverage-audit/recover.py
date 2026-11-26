"""Recover only finalized, previously unscored records; never restart inference."""
import hashlib,json,shutil
from pathlib import Path
from bench.accuracy.codex_mcp import executed_sql
root=Path('/Users/seyoung/workspace/benchmarks');audit=json.loads((root/'codex-mcp-coverage-audit-20261127/audit.json').read_text())
for suite,sub in [('bird','bird-gpt-6.2-sol'),('spider2','spider-gpt-6.2-sol')]:
 src=root/'codex-mcp-full-20261127'/sub;out=root/'codex-mcp-recovered-records-20261127'/sub
 m=json.loads((src/'manifest.json').read_text());unobserved=set(audit['summary'][suite]['unobserved']);records=[];identities=[]
 for f in sorted((src/'codex-mcp/records').glob('*.json')):
  r=json.loads(f.read_text());i=r['instance_id']
  if i not in unobserved:continue
  assert r['cli_exit_code']==0 and r['status'] in ('predicted','failed')
  raw=src/'codex-mcp/events'/f'{i}.jsonl';assert hashlib.sha256(raw.read_bytes()).hexdigest()==r['events_sha256'];events=[json.loads(l) for l in raw.read_text().splitlines()];assert any(e['type']=='turn.completed' for e in events)
  case=next(c for c in m['cases'] if c['instance_id']==i)
  if r['status']=='predicted':
   db=root/('bird-sqlite' if suite=='bird' else 'spider2-databases')/(case['db']+'.sqlite');assert executed_sql(r['answer'],events,db=db)==r['sql']
  else:assert r['error_message']=='Final answer must contain exactly one SQL block; no last-tool fallback'
  records.append((i,f));identities.append(case)
 out.mkdir(parents=True);m['cases']=identities;m['development_ids']=[i for i,f in records];m['holdout_ids']=[]
 (out/'manifest.json').write_text(json.dumps(m,indent=2)+'\n');shutil.copy2(src/'settings.json',out/'settings.json')
 files={}
 for i,f in records:
  for part in ['records','sql','csv','events']:
   for item in (src/'codex-mcp'/part).glob(i+'.*'):
    dest=out/'codex-mcp'/part/item.name;dest.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(item,dest);files[str(item)]=hashlib.sha256(item.read_bytes()).hexdigest()
 (out/'codex-mcp/status.json').write_text(json.dumps({'status':'completed','kind':'immutable completed-record subset recovered from interrupted full run','parent_run_still_interrupted':True,'new_model_calls':0,'cases':len(records)},indent=2)+'\n')
 (out/'recovery.json').write_text(json.dumps({'source':str(src),'selected_ids':[i for i,f in records],'reason':'Finalized turns missing from prior completed-scored coverage inventory','checks':['CLI exit0','turn.completed event','event SHA256','successful SQL matches exact MCP execution footer; failed records are genuine clarification answers'],'legacy_limit':'These records predate exact prompt capture; original user prompts are not reconstructed or claimed available. Original source/model revision is retained.','files':files},indent=2)+'\n');print(suite,len(records),[i for i,f in records])
