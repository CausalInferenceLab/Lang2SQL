import json

import pytest

from bench.accuracy.compare_traces import compare_runs, render_html
from bench.spider2.evaluate import write_json


def run_fixture(root, score):
    case = {'instance_id': 'case1', 'question': '<script>alert(1)</script>', 'db': 'db'}
    write_json(root / 'manifest.json', {'cases': [case], 'db_sha256': {'db': 'same'},
                                      'suite': 'bird', 'evaluator_sha256': 'grader'})
    write_json(root / 'settings.json', {'revision': 'test'})
    write_json(root / 'codex-mcp/status.json', {'status': 'completed'})
    write_json(root / 'codex-mcp/scores.json', [{'instance_id': 'case1', 'score': score}])
    write_json(root / 'codex-mcp/records/case1.json', {
        'instance_id': 'case1', 'status': 'predicted', 'rows': 1,
        'sql': 'SELECT ' + str(score), 'mcp_calls': [{'result': '<img src=x onerror=alert(1)>'}],
    })
    path = root / 'codex-mcp/csv/case1.csv'
    path.parent.mkdir()
    path.write_text('value\n' + str(score) + '\n')


def test_report_preserves_scores_diffs_results_and_escapes_trace_content(tmp_path):
    old, new = tmp_path / 'old', tmp_path / 'new'
    run_fixture(old, 0)
    run_fixture(new, 1)
    report = compare_runs(old, new)
    assert report['transitions'] == {'0 -> 1': 1}
    case = report['cases'][0]
    assert '-SELECT 0' in case['sql_diff'] and '+SELECT 1' in case['sql_diff']
    assert case['after']['result']['preview'] == [['1']]
    assert case['after']['record']['mcp_calls'][0]['result'].startswith('<img')
    rendered = render_html(report)
    assert '<script>' not in rendered and '<img ' not in rendered
    assert '&lt;script&gt;' in rendered and '&lt;img ' in rendered


@pytest.mark.parametrize('change', ['running', 'missing_score', 'question', 'db', 'grader', 'row_count', 'record_id', 'events'])
def test_report_rejects_incomplete_or_incompatible_evidence(tmp_path, change):
    old, new = tmp_path / 'old', tmp_path / 'new'
    run_fixture(old, 0)
    run_fixture(new, 0)
    if change == 'running':
        write_json(new / 'codex-mcp/status.json', {'status': 'running'})
    elif change == 'missing_score':
        write_json(new / 'codex-mcp/scores.json', [])
    elif change in {'question', 'db', 'grader'}:
        path = new / 'manifest.json'
        data = json.loads(path.read_text())
        if change == 'question': data['cases'][0]['question'] = 'changed'
        elif change == 'db': data['db_sha256']['db'] = 'different'
        else: data['evaluator_sha256'] = 'different'
        write_json(path, data)
    else:
        path = new / 'codex-mcp/records/case1.json'
        data = json.loads(path.read_text())
        if change == 'row_count': data['rows'] = 2
        elif change == 'record_id': data['instance_id'] = 'wrong'
        else:
            data['events_sha256'] = 'mismatch'
            write_json(new / 'codex-mcp/events/case1.jsonl', {'type': 'turn.completed'})
        write_json(path, data)
    with pytest.raises(ValueError):
        compare_runs(old, new)


def test_csv_preview_preserves_multiline_cells_and_reports_truncation(tmp_path):
    import csv

    old, new = tmp_path / 'old', tmp_path / 'new'
    run_fixture(old, 0)
    run_fixture(new, 0)
    path = new / 'codex-mcp/records/case1.json'
    record = json.loads(path.read_text())
    record['rows'] = 25
    write_json(path, record)
    with (new / 'codex-mcp/csv/case1.csv').open('w', newline='') as stream:
        writer = csv.writer(stream)
        writer.writerow(['value'])
        writer.writerows([['a,b\nsecond line']] * 25)
    result = compare_runs(old, new)['cases'][0]['after']['result']
    assert result['row_count'] == 25 and result['preview_truncated']
    assert len(result['preview']) == 20 and result['preview'][0] == ['a,b\nsecond line']


def test_clarification_without_csv_remains_visible(tmp_path):
    old, new = tmp_path / 'old', tmp_path / 'new'
    run_fixture(old, 0)
    run_fixture(new, 0)
    write_json(new / 'codex-mcp/records/case1.json', {
        'instance_id': 'case1', 'status': 'failed', 'answer': 'Which definition?',
        'error_message': 'No final SQL', 'mcp_calls': [],
    })
    (new / 'codex-mcp/csv/case1.csv').unlink()
    report = compare_runs(old, new)
    assert report['cases'][0]['after']['result'] is None
    assert 'Which definition?' in render_html(report)
