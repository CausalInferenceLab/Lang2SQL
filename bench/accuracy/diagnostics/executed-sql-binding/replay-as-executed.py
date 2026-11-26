import pathlib,json,hashlib
from bench.accuracy.codex_mcp import executed_sql
from bench.accuracy.evaluate import query_rows
root=pathlib.Path('/Users/seyoung/workspace/benchmarks');report={'purpose':'Offline replay of saved execution provenance; no model calls, no grader/old artifact mutation','runs':[]}
for name in ['codex-mcp-integrated-20261127','codex-mcp-prior-full-failures-20261127','codex-mcp-integrated-review-20261127']:
 for suite in ['bird','spider']:
  run=root/name/(suite+'-gpt-6.2-sol');manifest=json.loads((run/'manifest.json').read_text());cases={c['instance_id']:c for c in manifest['cases']};entry={'source':str(run),'predictions_checked':0,'changed_sql':[],'rejected':[]}
  for p in sorted((run/'codex-mcp/records').glob('*.json')):
   d=json.loads(p.read_text())
   if d['status']!='predicted':continue
   i=d['instance_id'];events=run/'codex-mcp/events'/(i+'.jsonl');assert hashlib.sha256(events.read_bytes()).hexdigest()==d['events_sha256']
   db=root/('bird-sqlite' if suite=='bird' else 'spider2-databases')/(cases[i]['db']+'.sqlite')
   raw=[json.loads(l) for l in events.read_text().splitlines() if l.startswith('{')]
   entry['predictions_checked']+=1
   try:new=executed_sql(d['answer'],raw,db=db)
   except ValueError as exc:
    entry['rejected'].append({'id':i,'error':str(exc)});continue
   if new!=d['sql']:
    assert hashlib.sha256(db.read_bytes()).hexdigest()==manifest['db_sha256'][cases[i]['db']]
    old_rows=query_rows(db,d['sql'],30,100000);new_rows=query_rows(db,new,30,100000)
    entry['changed_sql'].append({'id':i,'old_sql':d['sql'],'bound_sql':new,'old_rows':len(old_rows),'new_rows':len(new_rows),'native_rows_identical':old_rows==new_rows,'record_sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'events_sha256':d['events_sha256']})
  report['runs'].append(entry)
  print(name,suite,entry['predictions_checked'],'changed',[(x['id'],x['native_rows_identical']) for x in entry['changed_sql']],'rejected',len(entry['rejected']))
p=pathlib.Path('bench/accuracy/diagnostics/executed-sql-binding/replay.json');p.write_text(json.dumps(report,indent=2)+'\n')
