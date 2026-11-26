import ast, hashlib, json, math, sqlite3
from pathlib import Path
import pandas as pd
root=Path('/Users/seyoung/workspace/benchmarks');source=root/'codex-mcp-spider-max-calculations-20261127/spider-gpt-6.2-astra';gold=root/'Spider2/spider2-lite/evaluation_suite/gold'; evaluator=gold.parent/'evaluate.py'; db=root/'spider2-databases/IPL.sqlite'
ns={'pd':pd,'math':math}
tree=ast.parse(evaluator.read_text()); fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='compare_pandas_table');exec(compile(ast.Module(body=[fn],type_ignores=[]),str(evaluator),'exec'),ns)
config=next(json.loads(s) for s in (gold/'spider2lite_eval.jsonl').read_text().splitlines() if json.loads(s)['instance_id']=='local259')
sql=(source/'codex-mcp/sql/local259.sql').read_text();drop="    WHERE kind_out <> 'retired hurt'\n";old="bw.wickets || '-' || bw.runs_given AS best_bowling_performance";new="CASE WHEN bw.total_wickets > 0 THEN bw.wickets || '-' || bw.runs_given END AS best_bowling_performance"
assert sql.count(drop)==1 and sql.count(old)==1
variants={'original':sql,'include_retired_hurt':sql.replace(drop,''),'null_for_zero_career_wickets':sql.replace(old,new),'both_reference_conventions':sql.replace(drop,'').replace(old,new)}
con=sqlite3.connect('file:'+str(db)+'?mode=ro',uri=True)
results={}
refs=sorted((gold/'exec_result').glob('local259_*.csv'))
for name,q in variants.items():
 frame=pd.read_sql_query(q,con)
 results[name]={'rows':len(frame),'sql':q,'reference_matches':{f.name:ns['compare_pandas_table'](frame,pd.read_csv(f),config['condition_cols'],config['ignore_order']) for f in refs}}
retired=con.execute("SELECT player_out,COUNT(*) FROM wicket_taken WHERE kind_out='retired hurt' GROUP BY player_out").fetchall();con.close()
report={'purpose':'Offline root-cause isolation, never supplied to inference; original official score remains zero.','source':str(source),'hashes':{str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in [source/'codex-mcp/records/local259.json',source/'codex-mcp/sql/local259.sql',db,evaluator,*refs]},'config':config,'retired_hurt_counts':retired,'variants':results,'findings':['Original and reference_b both contain 468 players and 18 columns.','Reference_b mismatches are confined to dismissals/batting average for six retired-hurt players and best-bowling text for 47 zero-career-wicket players.','All other columns match reference_b by player ID within the official numeric tolerance.'],'limits':['Question and provided glossary do not explicitly define retirement treatment or null best-performance output for a bowler with zero wickets.','Counterfactual reference compatibility is not model accuracy or evidence of an execution-tool bug.','Do not inject reference-derived conventions into product policy.']}
out=root/'codex-mcp-current-backlog-audit-20261127/local259-boundary-audit.json';out.write_text(json.dumps(report,indent=2)+'\n')
for name,r in results.items():print(name,r['reference_matches'])
