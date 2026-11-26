import difflib,json
from pathlib import Path
from bench.accuracy.codex_mcp import _scored_cases
from bench.accuracy.compare_traces import _case_trace,render_html
from bench.spider2.evaluate import digest
root=Path('/Users/seyoung/workspace/benchmarks');current=root/'codex-mcp-bird-full-context-validation-20261127/checkpoints/001/bird';canonical=json.loads(Path('bench/accuracy/bird-manifest.json').read_text());cases,scores=_scored_cases(canonical['cases'],current,canonical['db_sha256']);ledger=json.loads((root/'codex-mcp-current-backlog-audit-20261127/audit.json').read_text());entries={e['instance_id']:e for e in ledger['cases']['bird']};report={'scope':'First completed-record snapshot of live full500 run, 100/106; NOT full accuracy. Historical successful attempts may use different versions/models. A 1->0 transition alone does not prove a code-induced regression.','sources':[],'transitions':{},'cases':[]};sources={current}
for case in cases:
 i=case['instance_id']
 if scores[i]['score']:continue
 observations=entries[i]['observations'];successes=[o for o in observations if o['score']];old=Path((successes or observations)[-1]['source']);_,old_scores=_scored_cases(canonical['cases'],old,canonical['db_sha256']);before=_case_trace(old,i,old_scores[i]['score']);after=_case_trace(current,i,0);transition=f'{before["score"]} -> 0';report['transitions'][transition]=report['transitions'].get(transition,0)+1;sources.add(old);report['cases'].append({'input':case,'transition':transition,'baseline_source':str(old),'before':before,'after':after,'sql_diff':'\n'.join(difflib.unified_diff((before['record'].get('sql') or '').splitlines(),(after['record'].get('sql') or '').splitlines(),fromfile='before.sql',tofile='after.sql',lineterm=''))})
for p in sorted(sources):report['sources'].append({'path':str(p),'manifest_sha256':digest(p/'manifest.json'),'scores_sha256':digest(p/'codex-mcp/scores.json'),'settings':json.loads((p/'settings.json').read_text())})
out=current.parent/'comparison';out.mkdir(exist_ok=False);(out/'comparison.json').write_text(json.dumps(report,indent=2)+'\n');(out/'index.html').write_text(render_html(report));print(report['transitions'])
