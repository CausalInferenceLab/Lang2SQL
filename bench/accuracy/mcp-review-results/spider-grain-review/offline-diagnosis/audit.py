"""Offline counterfactuals only; never model predictions or inference input."""
import ast, hashlib, json, math, sqlite3
from pathlib import Path
import pandas as pd

root=Path('/Users/seyoung/workspace/benchmarks')
run=root/'codex-mcp-astra-spider-backlog-20261127/spider-gpt-6.2-astra'
manifest=json.loads((run/'manifest.json').read_text());cases={c['instance_id']:c for c in manifest['cases']}
evaluator=root/'Spider2/spider2-lite/evaluation_suite/evaluate.py'
assert hashlib.sha256(evaluator.read_bytes()).hexdigest()==manifest['evaluator_sha256']
node=next(n for n in ast.parse(evaluator.read_text()).body if isinstance(n,ast.FunctionDef) and n.name=='compare_pandas_table')
ns={'pd':pd,'math':math};exec(compile(ast.Module(body=[node],type_ignores=[]),str(evaluator),'exec'),ns)
gold=evaluator.parent/'gold';standards={r['instance_id']:r for r in map(json.loads,(gold/'spider2lite_eval.jsonl').read_text().splitlines())}
report=[]
for ident in ['local015','local059','local283']:
 record=run/'codex-mcp/records'/f'{ident}.json';sql=json.loads(record.read_text())['sql'];changed=sql
 if ident=='local015':
  start=sql.index('SELECT\n    100.0')
  changed=sql[:start]+"SELECT CASE WHEN g.helmet_used = 1 THEN 'helmet_used' ELSE 'helmet_not_used' END AS helmet_usage, 100.0 * SUM(c.motorcyclist_killed_count) / COUNT(*) AS fatality_percentage\nFROM main.collisions AS c JOIN collision_groups AS g ON g.case_id = c.case_id\nWHERE c.motorcycle_collision = 1 GROUP BY g.helmet_used ORDER BY g.helmet_used DESC"
 elif ident=='local059':
  changed=changed.replace('SELECT total_quantity,','SELECT division, total_quantity,').replace('SELECT AVG(total_quantity)','SELECT division, AVG(total_quantity)').replace('WHERE division_rank <= 3\nLIMIT 1','WHERE division_rank <= 3\nGROUP BY division\nORDER BY division')
 else:
  changed=changed.replace('PARTITION BY season ORDER BY total_points DESC','PARTITION BY season, league_id ORDER BY total_points DESC')
 assert changed!=sql
 db=root/'spider2-databases'/(cases[ident]['db']+'.sqlite');assert hashlib.sha256(db.read_bytes()).hexdigest()==manifest['db_sha256'][cases[ident]['db']]
 with sqlite3.connect('file:'+str(db)+'?mode=ro',uri=True) as conn:
  old=pd.read_sql_query(sql,conn);new=pd.read_sql_query(changed,conn)
 refs=sorted((gold/'exec_result').glob(ident+'*.csv'));cond=standards[ident].get('condition_cols');ignore=standards[ident].get('ignore_order',False)
 if cond in (None,[],[[]],[None]):conds=[[] for _ in refs]
 elif len(refs)>1 and not all(isinstance(x,list) for x in cond):conds=[cond for _ in refs]
 else:conds=cond if len(refs)>1 else [cond]
 comparisons=[]
 for p,cols in zip(refs,conds):
  ref=pd.read_csv(p);required=cols or list(range(len(ref.columns)))
  comparisons.append({'reference':p.name,'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'rows':len(ref),'counterfactual_matches':bool(ns['compare_pandas_table'](new,ref,cols,ignore)), 'unmatched_columns':[str(ref.columns[i]) for i in required if not ns['compare_pandas_table'](new,ref,[i],ignore)]})
 if ident=='local283':
  reference=pd.read_csv(refs[0])
  paired=new.rename(columns={'team_name':'champion_team'}).merge(reference,on=['season','league','country','champion_team'],how='outer',suffixes=('_pred','_ref'),indicator=True)
  diff=paired[(paired['_merge']!='both') | (paired.total_points_pred!=paired.total_points_ref)].copy()
  diff.to_csv(Path(__file__).parent/'local283-team-differences.csv',index=False)
 out=Path(__file__).parent;new.to_csv(out/(ident+'-counterfactual.csv'),index=False);(out/(ident+'-counterfactual.sql')).write_text(changed)
 report.append({'id':ident,'question':cases[ident]['question'],'record_sha256':hashlib.sha256(record.read_bytes()).hexdigest(),'db_sha256':manifest['db_sha256'][cases[ident]['db']],'original_rows':len(old),'counterfactual_rows':len(new),'change':{'local015':'Unpivot the same numeric rates into helmet groups using descriptive labels derived from prior query, not reference strings','local059':'Carry division to final AVG and GROUP BY division','local283':'Partition season rank by both season and league_id'}[ident],'comparisons':comparisons})
(Path(__file__).parent/'audit.json').write_text(json.dumps({'scope':'Offline diagnostic only, not a model repair score. No reference-dependent edits beyond the explicitly recorded grain hypothesis. Never passed to inference.','evaluator_sha256':manifest['evaluator_sha256'],'cases':report},indent=2)+'\n')
print(json.dumps(report,indent=2))
