from pathlib import Path
import collections,hashlib,itertools,json,math
from bench.accuracy.evaluate import query_rows,load_bird_comparator
root=Path('/Users/seyoung/workspace/benchmarks');run=root/'codex-mcp-integrated-20261127/bird-gpt-6.2-sol';manifest=json.loads((run/'manifest.json').read_text());qfile=root/'bird-mini-dev-20261025.jsonl';assert hashlib.sha256(qfile.read_bytes()).hexdigest()==manifest['question_file_sha256'];gold={'bird%03d'%x['question_id']:x for x in map(json.loads,qfile.read_text().splitlines())};compare=load_bird_comparator(root/'bird-evaluation.py',manifest['evaluator_sha256']);cases={c['instance_id']:c for c in manifest['cases']};audit=[]
for scored in json.loads((run/'codex-mcp/scores.json').read_text()):
 id=scored['instance_id'];record=json.loads((run/'codex-mcp/records'/(id+'.json')).read_text());item={'instance_id':id,'question':cases[id]['question'],'score':scored['score'],'record_sha256':hashlib.sha256((run/'codex-mcp/records'/(id+'.json')).read_bytes()).hexdigest()}
 if scored['score']==1:item['category']='correct'
 elif record['status']!='predicted':item.update(category='no_final_sql',answer=record.get('answer',''),error=record.get('error_message'))
 else:
  db=root/'bird-sqlite'/(cases[id]['db']+'.sqlite')
  try:
   pred=query_rows(db,record['sql'],30,100000);ref=query_rows(db,gold[id]['SQL'],30,100000);assert compare(pred,ref)==scored['score']
   item.update(predicted_rows=len(pred),reference_rows=len(ref),predicted_width=len(pred[0]) if pred else None,reference_width=len(ref[0]) if ref else None,category='value_or_population_mismatch')
   if pred and ref and len(ref[0])<=len(pred[0])<=8:
    for indices in itertools.permutations(range(len(pred[0])),len(ref[0])):
     if compare([tuple(row[i] for i in indices) for row in pred],ref):
      item.update(category='projection_only_mismatch',matching_predicted_columns=indices);break
   if item['category']=='value_or_population_mismatch' and len(pred)==len(ref) and pred and len(pred[0])==len(ref[0]):
    def close(a,b):
     return math.isclose(a,b,rel_tol=1e-12,abs_tol=1e-12) if isinstance(a,(int,float)) and isinstance(b,(int,float)) else a==b
    if all(all(close(a,b) for a,b in zip(p,q)) for p,q in zip(sorted(pred,key=str),sorted(ref,key=str))):item['category']='tiny_numeric_representation_difference'
  except Exception as exc:
   item.update(category='diagnostic_error',error=f'{type(exc).__name__}: {exc}')
 audit.append(item)
out=run.parent/'bird-result-audit.json';out.write_text(json.dumps({'source_run':str(run),'evaluator_sha256':manifest['evaluator_sha256'],'scope':'diagnostic only; unchanged official scores and model inputs','counts':dict(collections.Counter(x['category'] for x in audit)),'cases':audit},ensure_ascii=False,indent=2)+'\n');print(out);print(collections.Counter(x['category'] for x in audit))
for x in audit:
 if x['category'] not in ('correct','value_or_population_mismatch'):print(x['instance_id'],x['category'])
