import ast,hashlib,json,math,collections
from pathlib import Path
import pandas as pd
root=Path('/Users/seyoung/workspace/benchmarks');run=root/'codex-mcp-astra-spider-backlog-20261127/spider-gpt-6.2-astra';script=root/'Spider2/spider2-lite/evaluation_suite/evaluate.py';manifest=json.loads((run/'manifest.json').read_text());assert hashlib.sha256(script.read_bytes()).hexdigest()==manifest['evaluator_sha256']
# Execute the pinned comparator function unchanged, without evaluator import side effects.
node=next(n for n in ast.parse(script.read_text()).body if isinstance(n,ast.FunctionDef) and n.name=='compare_pandas_table');ns={'pd':pd,'math':math};exec(compile(ast.Module(body=[node],type_ignores=[]),str(script),'exec'),ns);compare=ns['compare_pandas_table']
gold=script.parent/'gold';standards={d['instance_id']:d for d in map(json.loads,(gold/'spider2lite_eval.jsonl').read_text().splitlines())};cases={c['instance_id']:c for c in manifest['cases']};audit=[]
for scored in json.loads((run/'codex-mcp/scores.json').read_text()):
 id=scored['instance_id'];record=json.loads((run/'codex-mcp/records'/(id+'.json')).read_text());item={'instance_id':id,'question':cases[id]['question'],'score':scored['score'],'record_sha256':hashlib.sha256((run/'codex-mcp/records'/(id+'.json')).read_bytes()).hexdigest()}
 if scored['score']==1:item['category']='correct'
 elif record['status']!='predicted':item.update(category='no_final_sql',answer=record.get('answer',''),error=record.get('error_message'))
 else:
  pred=pd.read_csv(run/'codex-mcp/csv'/(id+'.csv'));paths=sorted((gold/'exec_result').glob(id+'*.csv'));cond=standards[id].get('condition_cols');order=standards[id].get('ignore_order',False)
  if cond in (None,[],[[]],[None]):conds=[[] for _ in paths]
  elif len(paths)>1 and not all(isinstance(x,list) for x in cond):conds=[cond for _ in paths]
  else:conds=cond if len(paths)>1 else [cond]
  variants=[]
  for path,cols in zip(paths,conds):
   ref=pd.read_csv(path);required=cols or list(range(len(ref.columns)));matched=[];missing=[]
   for col in required:
    (matched if compare(pred,ref,[col],order) else missing).append(str(ref.columns[col]))
   variants.append({'reference':path.name,'rows':len(ref),'required_columns':[str(ref.columns[x]) for x in required],'matched_columns':matched,'unmatched_columns':missing})
  item.update(category='row_count_mismatch' if all(v['rows']!=len(pred) for v in variants) else 'column_values_or_projection_mismatch',predicted_rows=len(pred),predicted_columns=list(pred.columns),variants=variants)
 audit.append(item)
out=run.parent/'spider-result-audit.json';out.write_text(json.dumps({'source_run':str(run),'evaluator_sha256':manifest['evaluator_sha256'],'scope':'diagnostic analysis only; no changes to official grading or model inputs','counts':dict(collections.Counter(x['category'] for x in audit)),'cases':audit},ensure_ascii=False,indent=2)+'\n')
print(out);print(collections.Counter(x['category'] for x in audit))
for item in audit:
 if 'variants' in item:
  best=min(item['variants'],key=lambda x:(len(x['unmatched_columns']),abs(x['rows']-item['predicted_rows'])));print(item['instance_id'],item['category'],'rows',item['predicted_rows'],'ref',best['rows'],'unmatched',best['unmatched_columns'])
