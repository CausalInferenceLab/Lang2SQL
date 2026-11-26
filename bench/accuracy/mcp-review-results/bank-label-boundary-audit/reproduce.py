import ast,hashlib,json,math,sqlite3
from pathlib import Path
import pandas as pd
root=Path('/Users/seyoung/workspace/benchmarks');gold=root/'Spider2/spider2-lite/evaluation_suite/gold';evaluator=gold.parent/'evaluate.py';ns={'pd':pd,'math':math}
fn=next(n for n in ast.parse(evaluator.read_text()).body if isinstance(n,ast.FunctionDef) and n.name=='compare_pandas_table');exec(compile(ast.Module(body=[fn],type_ignores=[]),str(evaluator),'exec'),ns)
configs={x['instance_id']:x for x in map(json.loads,(gold/'spider2lite_eval.jsonl').read_text().splitlines())}
report={'purpose':'Offline diagnosis only; no changes to original score, model input, production behavior or official evaluator.','evaluator_sha256':hashlib.sha256(evaluator.read_bytes()).hexdigest(),'cases':{}}
for ident,prefix,db in [('local299','spider-max-calculations','bank_sales_trading'),('local220','astra-spider-backlog','EU_soccer')]:
 source=root/f'codex-mcp-{prefix}-20261127/spider-gpt-6.2-astra';p=source/'codex-mcp/sql'/f'{ident}.sql';sql=p.read_text();cfg=configs[ident];refs=sorted((gold/'exec_result').glob(ident+'_*.csv'))
 if ident=='local299':
  bounded=sql.replace('(SELECT MAX(txn_date) FROM daily_changes)','(SELECT MAX(dc.txn_date) FROM daily_changes dc WHERE dc.customer_id=customer_days.customer_id)');assert bounded!=sql
  variants={'original':sql,'per_customer_horizon':bounded,'per_customer_round3':bounded.replace('SUM(max_avg_balance)','SUM(ROUND(max_avg_balance,3))'),'per_customer_truncate3':bounded.replace('SUM(max_avg_balance)','SUM(CAST(max_avg_balance*1000 AS INTEGER)/1000.0)')}
 else:variants={'original':sql,'uppercase_output_labels':sql.replace('SELECT r.outcome, p.player_name','SELECT UPPER(r.outcome) AS outcome, p.player_name')}
 dbpath=root/'spider2-databases'/f'{db}.sqlite';c=sqlite3.connect('file:'+str(dbpath)+'?mode=ro',uri=True);results={}
 for name,q in variants.items():
  frame=pd.read_sql_query(q,c);results[name]={'sql':q,'columns':list(frame.columns),'rows':frame.values.tolist(),'reference_matches':{f.name:ns['compare_pandas_table'](frame,pd.read_csv(f),cfg['condition_cols'],cfg['ignore_order']) for f in refs}}
 c.close()
 report['cases'][ident]={'source':str(source),'config':cfg,'hashes':{str(f):hashlib.sha256(f.read_bytes()).hexdigest() for f in [p,source/'codex-mcp/records'/f'{ident}.json',dbpath,*refs]},'variants':results}
report['cases']['local299']['interpretation']='Customer-specific end date removes large monthly differences. Truncating each customer monthly maximum to three decimals additionally matches reference within official tolerance; rounding does not. The question does not specify three-decimal intermediate truncation. Do not turn this reference-derived convention into a production default.'
report['cases']['local220']['interpretation']='Names and match counts are already correct. Uppercasing only synthetic Win/Loss labels matches reference_c; the question does not prescribe label case. This is not an SQL execution fault and does not justify case-insensitive regrading or automatic uppercasing of database values.'
out=root/'codex-mcp-current-backlog-audit-20261127/bank-and-label-boundary-audit.json';out.write_text(json.dumps(report,indent=2)+'\n')
for ident,data in report['cases'].items():
 for variant,r in data['variants'].items():print(ident,variant,[k for k,v in r['reference_matches'].items() if v])
