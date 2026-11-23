"""Render the experiment report and PR body from the verified comparison JSON."""

from __future__ import annotations

import argparse
from pathlib import Path

from report_schema_recovery import STATES, load

NAME = {"spider2": "Spider 2.0-Lite SQLite", "bird": "BIRD Mini-Dev SQLite"}
REFERENCE = {"bird": 276, "spider2": 277}
PRODUCT = "fb1f36e9b0980c5e593d2fddb5ecc6607cd101d7"
SOURCE = {
    "spider2": "24cff8cb1126fc017d1c68389b76213858e3a2e6",
    "bird": "1f47f2821559992717aeb23e1cd34ca489142300",
}
BRANCH = {
    "spider2": "experiment/278-spider2-schema-recovery",
    "bird": "experiment/278-bird-schema-recovery",
}
RUNTIME = {
    "spider2": "두 GPT‑4.1 프로필에서 OpenAI SDK 3.51.0을 사용했다.",
    "bird": "두 GPT‑4.1 프로필에서 OpenAI SDK 3.51.0, GPT‑6 Luna에서 3.57.0을 사용했다.",
}
REPRO = {
    "spider2": r"""```bash
# 평가기 의존성은 원래 실행기의 잠금 파일에서 설치
git worktree add --detach "$DEPS" 24cff8cb1126fc017d1c68389b76213858e3a2e6
uv sync --project "$DEPS" --locked --extra benchmark --python 3.13.14
PY="$DEPS/.venv/bin/python"
export PYTHONPATH="$PWD/src:$PWD"
export LITELLM_LOCAL_MODEL_COST_MAP=True LITELLM_LOG=ERROR

git diff --exit-code fb1f36e9b0980c5e593d2fddb5ecc6607cd101d7 -- src/
for PROFILE in openai-gpt41-mini openai-gpt41; do
  "$PY" -c 'import json,sys; print(json.dumps([p for p in json.load(open(sys.argv[1])) if p["name"] == sys.argv[2]]))' \
    bench/spider2/models.example.json "$PROFILE" > "$PROFILE_JSON"
  "$PY" -m bench.spider2.evaluate run --root "$SPIDER" --db-dir "$DB_DIR" \
    --manifest bench/spider2/manifest-sqlite135.json --profiles "$PROFILE_JSON" \
    --env-file "$CREDENTIALS" --output "$RUN/$PROFILE" --max-calls 8 \
    --concurrency 4 --call-timeout 120 --case-timeout 480 --sql-timeout 30 --max-rows 100000
  "$PY" -m bench.spider2.evaluate score --root "$SPIDER" --output "$RUN/$PROFILE"
done
```

`SPIDER`는 고정 리비전의 Spider2 체크아웃 경로다. `DB_DIR`에 30개 SQLite 파일을 둔다.
`DEPS`, `RUN`, `PROFILE_JSON`, `CREDENTIALS`는 각각 의존성 워크트리, 새 실행 루트,
프로필 설정 임시 파일, 기존 인증 설정 파일의 절대 경로다.
이어서 실행할 때 같은 `run`에 `--resume`을 추가한다.
""",
    "bird": r"""```bash
# #276의 잠금 파일과 두 SDK 버전 사용
git worktree add --detach "$DEPS" 8c1cb0b
uv sync --project "$DEPS" --locked --extra benchmark --python 3.13.14
PY="$DEPS/.venv/bin/python"
uv pip install --python "$PY" --target "$SDK_351" --no-deps openai==3.51.0
export PYTHONPATH="$PWD/src:$PWD"
export LITELLM_LOCAL_MODEL_COST_MAP=True LITELLM_LOG=ERROR

"$PY" -m bench.bird.evaluate prepare --source "$SOURCE" --db-dir "$DB_DIR" \
  --db-revision 8132aacfe61ecec7198a62e93cb8512dac89b1d6b9922fe3c7bb94b13f317d8f \
  --output "$RUN/prepared"
cmp "$RUN/prepared/manifest.json" bench/bird/results/schema-278-v1/manifest.json
"$PY" -m bench.bird.evaluate score --gold-only --source "$SOURCE" \
  --db-dir "$DB_DIR" --output "$RUN/prepared"

git diff --exit-code fb1f36e9b0980c5e593d2fddb5ecc6607cd101d7 -- src/
lane() (
  PROFILE=$1
  if test "$PROFILE" != openai-gpt6-luna; then
    export PYTHONPATH="$PYTHONPATH:$SDK_351"
  fi
  for CONDITION in A B D; do
    OUT="$RUN/$CONDITION-$PROFILE"
    "$PY" -m bench.bird.evaluate run --condition "$CONDITION" --profile "$PROFILE" \
      --prepared "$RUN/prepared" --db-dir "$DB_DIR" --env-file "$CREDENTIALS" \
      --product-commit fb1f36e9b0980c5e593d2fddb5ecc6607cd101d7 --output "$OUT" || return
    "$PY" -m bench.bird.evaluate score --source "$SOURCE" --db-dir "$DB_DIR" \
      --output "$OUT" || return
  done
)
lane openai-gpt41-mini &
MINI_PID=$!
lane openai-gpt41 &
FULL_PID=$!
lane openai-gpt6-luna &
LUNA_PID=$!
wait "$MINI_PID"
wait "$FULL_PID"
wait "$LUNA_PID"
```

`SOURCE`는 고정 SQLite JSONL 파일이고 `DB_DIR`은 같은 DB 스냅샷 11개를 담은 경로다.
`DEPS`, `SDK_351`, `RUN`, `CREDENTIALS`는 각각 의존성 워크트리, GPT‑4.1 SDK 설치 경로,
새 실행 루트, 기존 인증 설정 파일의 절대 경로다.
이어서 실행할 때 같은 조합의 `run`에 `--resume`을 추가한다.
""",
}


def table(headers, rows):
    return "\n".join(
        "| " + " | ".join(map(str, row)) + " |"
        for row in [headers, ["---"] * len(headers), *rows]
    )


def show(value):
    if value is None:
        return "미수집"
    return f"{value:,}" if isinstance(value, int) else f"{value:.2f}"


def moved(group, field):
    return " → ".join(show(group[arm].get(field)) for arm in ("baseline", "treatment"))


def state(group, name):
    return " → ".join(
        show(group[arm]["states"][name]) for arm in ("baseline", "treatment")
    )


def condition_header(benchmark):
    if benchmark == "bird":
        return (
            "A는 evidence를 `term_custom`으로 등록해 시스템 메시지에 주입했다. "
            "B는 같은 evidence를 질문에 첨부했고 D는 질문만 전달했다. "
            "새 세션에서 `run_sql`과 `explore_schema`를 제공했다. "
            "A의 `term_custom`은 문항별 입력 준비에 사용했고, 표의 도구 수는 모델 응답에서 요청한 도구를 기준으로 집계했다. "
            "EX는 예측과 기준 SQL의 행 튜플 집합이 일치하면 1이다. "
            "`gpt-6-luna`에는 `reasoning_effort=none`을 전달했다. "
            "모델별로 A, B, D 순서로 실행했고 세 모델은 동시에 실행했다."
        )
    return (
        "같은 SQLite DB 30개, 공식 정답과 문항별 외부 문서를 사용했다. "
        "#260의 결과 CSV 생성 절차와 Spider2 공식 채점 함수로 판정했다. "
        "GPT‑4.1 mini를 실행·채점한 뒤 GPT‑4.1을 실행·채점했다."
    )


def settings_header(benchmark, data, manifest, groups, prefix):
    is_bird = benchmark == "bird"
    reference = REFERENCE[benchmark]
    dataset = (
        manifest["cohort"]["dataset_revision"]
        if is_bird
        else manifest["upstream_commit"]
    )
    n_groups = len(groups)
    planned = data["planned_pairs"]
    source = "bench.bird.evaluate" if is_bird else "bench.spider2.evaluate"
    end = max(group["sources"]["treatment"]["scores_saved_utc"] for group in groups)
    start = min(group["treatment"]["started_at"] for group in groups)
    old_start = min(
        group["sources"]["baseline"]["settings_saved_utc"] for group in groups
    )
    old_end = max(group["sources"]["baseline"]["scores_saved_utc"] for group in groups)
    heads = sorted(
        {group["sources"]["treatment"]["evaluator_git_head"] for group in groups}
    )
    evaluator_commits = ", ".join(f"`{head}`" for head in heads)
    lines = [
        f"저장된 #275 기준군과 새 #278 변경군의 {NAME[benchmark]} 비교다. "
        f"기준군은 #{reference}에 변경군으로 게시된 `{data['baseline_product']}`이고, "
        f"변경군은 #279의 `{data['treatment_product']}`이다. "
        f"변경군에서 {groups[0]['pairs']:,}문항 × {n_groups}개 조합, 총 {planned:,}건을 실행했다. "
        f"[기준군 PR](https://github.com/CausalInferenceLab/Lang2SQL/pull/{reference}) · "
        "[제품 PR #279](https://github.com/CausalInferenceLab/Lang2SQL/pull/279)",
        f"평가기 `{source}`의 원본 버전은 `{SOURCE[benchmark]}`다. "
        f"`{BRANCH[benchmark]}`의 실행기 커밋은 {evaluator_commits}다. "
        "실행 전 `src/`를 제품 SHA와 대조했다. "
        "조합별 실행기 소스 해시와 런타임을 설정에 남겼다. " + RUNTIME[benchmark],
        "Python 3.13.14, SQLite 3.53.1, LiteLLM 1.109.1, SQLAlchemy 2.0.50을 사용했다. "
        "문항당 모델 API 호출 8회에 강제 조회와 보조 호출을 포함했다. "
        "호출당 출력 4,096토큰, 동시성 조합당 4, API·문항·SQL 제한 120·480·30초와 "
        "최종 결과 상한 100,000행, 공급자 자동 재시도 0회를 적용했다.",
        condition_header(benchmark),
        f"데이터 리비전은 `{dataset}`, 보관한 manifest SHA-256은 "
        f"`{data['manifest_artifact_sha256']}`다. 주석·DB별 해시"
        + (
            "와 채점기 및 SQLite 연결 해시를 manifest와 설정에 기록했다."
            if is_bird
            else "와 문서·공식 채점기·정답·채점 설정 해시를 manifest에 기록했다."
        )
        + f" [Manifest]({prefix}manifest.json) · [의존성 잠금 파일과 패키지 버전]({prefix}checks/)",
        f"변경군 첫 문항 시작부터 마지막 채점 파일 저장까지 UTC `{start}` ~ `{end}`다. "
        f"기준군 원본 설정·채점 파일 시각 범위는 UTC `{old_start}` ~ `{old_end}`다.",
    ]
    if is_bird:
        lines.append(
            "[BIRD EX 채점 규칙](https://github.com/bird-bench/mini_dev/blob/1c49a375e76c471e218564da3f4617513d33e371/evaluation/evaluation_ex.py#L17)"
        )
        lines.append(
            f"준비한 manifest가 #276 원본과 일치했고 기준 SQL 365/365건이 전수 검사를 통과했다. "
            f"A/B/D의 기존 dry run은 각각 365/365건을 완료했다. [입력과 dry run 검사]({prefix}checks/)"
        )
    if not is_bird:
        lines.append(
            "[Spider2 공식 채점기](https://github.com/xlang-ai/Spider2/blob/cafb867313aab4e674652054198f383cf4018943/spider2-lite/evaluation_suite/evaluate.py)"
        )
    lines.append(
        "모의 테이블 조회로 첫 요청 auto, 최종 SQL·run_sql 보류, 강제 조회와 컬럼 응답 후 auto, "
        f"같은 응답의 조회·실행 요청을 확인했다. 모델별 강제 호출과 복귀 요청을 실호출했다. "
        f"실호출의 HTTP JSON에 `tool_choice`와 출력 상한을 확인했다. [모의·실호출 기록]({prefix}checks/)"
    )
    return "\n\n".join(lines)


def group_link(group, prefix):
    return f"[{group['group']}]({prefix}{group['group']}/paired-scores.jsonl)"


def accuracy(groups, prefix):
    return table(
        [
            "프로필·조건",
            "#275 정답/전체 (정확도)",
            "#278 정답/전체 (정확도)",
            "변화(%p)",
            "#275만 정답",
            "#278만 정답",
            "양쪽 정답",
            "양쪽 오답",
            "비교 쌍",
        ],
        [
            [
                group_link(g, prefix),
                *[
                    f"{g[a]['correct']}/{g['pairs']} ({g[a]['accuracy_pct']:.2f}%)"
                    for a in ("baseline", "treatment")
                ],
                f"{g['delta_percentage_points']:+.2f}",
                g["baseline_only"],
                g["treatment_only"],
                g["both_correct"],
                g["both_wrong"],
                g["pairs"],
            ]
            for g in groups
        ],
    )


def runs(groups, prefix):
    return table(
        [
            "프로필·조건",
            "계획",
            "시도 문항",
            "총 시도",
            "완료",
            "채점",
            "API 오류 호출",
            "중단",
            "재실행",
            "누락",
        ],
        [
            [
                group_link(g, prefix),
                *[
                    moved(g, key)
                    for key in (
                        "planned",
                        "attempted",
                        "total_attempts",
                        "completed",
                        "scored",
                        "api_errors",
                        "interrupted_attempts",
                        "rerun_questions",
                        "missing",
                    )
                ],
            ]
            for g in groups
        ],
    )


def submissions(groups, prefix):
    sql_states = [
        "result_mismatch",
        "no_such_column",
        "other_sql_error",
        "sql_timeout",
        "unclassified_sql_failure",
        "scorer_error",
    ]
    return table(
        [
            "프로필·조건",
            "최종 SQL 제출",
            "미제출",
            "형식 오류",
            "SQL 실행 성공",
            *(STATES[s] for s in sql_states),
        ],
        [
            [
                group_link(g, prefix),
                *[
                    moved(g, key)
                    for key in (
                        "submitted",
                        "not_submitted",
                        "invalid_format",
                        "sql_execution_success",
                    )
                ],
                *[
                    (
                        f"미수집 → {show(g['treatment']['states'][key])}"
                        if key == "sql_timeout"
                        and g["baseline"]["only_known_sql_timeouts"]
                        else state(g, key)
                    )
                    for key in sql_states
                ],
            ]
            for g in groups
        ],
    )


def calls(groups, prefix):
    return table(
        [
            "프로필·조건",
            "모델 호출",
            "도구 요청",
            "도구 실행",
            "run_sql 요청",
            "run_sql 실행",
            "도구 DB 실행",
            "최종 SQL DB 실행",
            "입력 토큰",
            "출력 토큰",
            "P50 (초)",
            "P95 (초)",
            "8회 도달",
            "예산 소진 미제출",
        ],
        [
            [
                group_link(g, prefix),
                *[
                    moved(g, key)
                    for key in (
                        "model_calls",
                        "tool_requests",
                        "tool_dispatches",
                        "run_sql_requests",
                        "run_sql_dispatches",
                        "db_executions",
                        "final_db_executions",
                        "input_tokens",
                        "output_tokens",
                        "latency_p50_seconds",
                        "latency_p95_seconds",
                        "reached_8",
                        "budget_without_sql",
                    )
                ],
            ]
            for g in groups
        ],
    )


def recovery(groups, prefix):
    def row(group):
        r = group["treatment"]["recovery"]
        return [
            group_link(group, prefix),
            *[
                r[key]
                for key in (
                    "withheld_run_sql",
                    "withheld_final_sql",
                    "same_response_columns",
                    "forced_schema_requests",
                    "forced_column_responses",
                    "column_responses",
                )
            ],
            f"{r['rewritten_submissions']}/{group['pairs']}",
            f"{r['rewritten_correct']}/{r['rewritten_submissions']}",
        ]

    return table(
        [
            "프로필·조건",
            "run_sql 보류",
            "최종 SQL 보류",
            "같은 응답에서 컬럼 확보",
            "강제 조회 요청",
            "강제 요청 후 컬럼 확보",
            "전체 응답 중 컬럼 확보",
            "재작성 제출/문항",
            "재작성 정답/제출",
        ],
        [row(g) for g in groups],
    )


def transitions(groups, prefix):
    active = {
        name
        for g in groups
        for name, count in g["state_transitions"]["no_such_column"].items()
        if count > 0
    }
    active.update(
        (
            "correct",
            "result_mismatch",
            "no_such_column",
            "other_sql_error",
            "sql_timeout",
        )
    )
    columns = [name for name in STATES if name in active]
    return table(
        ["프로필·조건", "#275 컬럼 오류", *(STATES[s] for s in columns)],
        [
            [
                group_link(g, prefix),
                g["baseline"]["states"]["no_such_column"],
                *[g["state_transitions"]["no_such_column"][s] for s in columns],
            ]
            for g in groups
        ],
    )


def links(groups, prefix):
    def link(group, arm, field, label):
        return f"[{label}]({prefix}{group['group']}/{arm}/{field})"

    return table(
        [
            "프로필·조건",
            "#275 문항별 기록 · 설정 · 채점",
            "#278 문항별 기록 · 설정 · 채점",
        ],
        [
            [
                group_link(g, prefix),
                *[
                    " · ".join(
                        link(g, a, f, label)
                        for f, label in (
                            ("records.jsonl", "원본"),
                            ("settings.json", "설정"),
                            ("scores.json", "채점"),
                        )
                    )
                    for a in ("baseline", "treatment")
                ],
            ]
            for g in groups
        ],
    )


def response_models(groups, prefix):
    def response(arm):
        return (
            ", ".join(
                f"`{model}` {count:,}회"
                for model, count in arm["response_models"].items()
            )
            or "미수집"
        )

    return table(
        [
            "프로필·조건",
            "프로필 요청 ID",
            "#275 응답 모델 ID",
            "#278 응답 모델 ID",
            "모델 ID 미수집 호출",
        ],
        [
            [
                group_link(g, prefix),
                f"`{g['requested_model']}`",
                response(g["baseline"]),
                response(g["treatment"]),
                moved(g, "missing_response_model_calls"),
            ]
            for g in groups
        ],
    )


def render(benchmark, data, manifest, *, pr_body=False):
    groups = list(data["groups"].values())
    prefix = f"results/schema-278-v1/"
    comparative = (
        "화살표는 기준군 → 변경군이다. 정확도의 분모는 계획 문항 수이며 미제출·실패도 포함한다. "
        "변화폭은 반올림 전 변경군 정확도에서 기준군 정확도를 뺀 값이다. "
        f"전체 비교 쌍은 {data['paired_cases']:,}/{data['planned_pairs']:,}건이다."
    )
    n_scored = sum(g["treatment"]["scored"] for g in groups)
    completed = sum(g["treatment"]["completed"] for g in groups)
    errors = sum(g["treatment"]["api_errors"] for g in groups)
    api_errors = ", ".join(
        f"{g['group']} "
        + ", ".join(
            f"{kind} {n}회" for kind, n in g["treatment"]["api_error_types"].items()
        )
        for g in groups
        if g["treatment"]["api_error_types"]
    )
    reruns = sum(g["treatment"]["rerun_questions"] for g in groups)
    state_note = (
        "SQL 실행 성공은 공식 정답·결과 불일치 및 실행 후 채점기 오류를 포함한다. "
        "형식 오류는 미제출의 부분집합이다. 제출 파서가 여러 SQL 블록 또는 SELECT/WITH가 아닌 블록을 거부했거나 "
        "답변에 SQL 블록 시작 표시 또는 줄 시작의 SELECT/WITH가 남아 있는데 파싱에 실패한 경우로 재집계했다. "
        "최종 컬럼 오류는 실패한 제출 SQL을 같은 읽기 전용 DB에서 `EXPLAIN QUERY PLAN`으로 준비해 "
        "`no such column`을 확인했다."
    )
    if benchmark == "spider2":
        state_note += (
            " #277 원본에 SQLite 실행 오류 코드가 빠져 있다. "
            "준비에 성공했으나 실행이 실패한 세 건의 원인은 미분류로 기록했다. "
            "그 세 건의 시간 초과 여부는 미수집이다."
        )
    call_note = (
        "재집계에는 원본 JSONL을 사용했다. #275 도구 요청은 assistant transcript, "
        "실행은 도구 결과 또는 도구 실행 이벤트에서 계산했다. #278의 도구 요청은 API 응답, "
        "실행은 도구 레지스트리 호출에서 계산했다. 도구 DB 실행은 `run_sql` 경로에서 SQLite 커서 실행에 "
        "도달한 횟수다. #275의 실제 도구 DB 실행과 스키마 복구 이벤트는 미수집이다. "
        "최종 DB 실행은 각 평가기의 제출 SQL 평가 시도 수다. 스키마 검사, 기준 SQL과 진단용 EXPLAIN 실행은 그 수에서 제외한다. "
        "토큰은 응답 사용량 합계다. P50·P95는 기록된 문항 경과 시간의 정렬값을 선형 보간했다. "
        "Spider2 경과 시간에는 최종 SQL 내보내기가 포함되고 BIRD 경과 시간은 생성 단계에서 측정했다."
    )
    recovery_note = (
        "복구 표는 #278 관측값이다. 보류와 조회 수는 한 문항에서 여러 번 증가할 수 있다. "
        "컬럼 확보는 `tables_with_columns`가 한 개 이상 반환된 모델 응답 단위로 집계했다. "
        "재작성 SQL 제출은 앞서 SQL이 보류되고 실제 컬럼을 얻은 문항에서 최종 SQL을 제출한 경우다."
    )
    time_note = "모의 응답과 별도 실호출은 본평가의 시도·호출·채점에서 제외했다."
    if benchmark == "spider2":
        time_note += (
            " GPT‑4.1의 local022·local066·local130은 원본 실행기가 `provider_error`로 저장했다. "
            "세 문항의 모델 호출 8회가 모두 성공했고 보조 호출이 예산에 포함됐다. "
            "9번째 호출 전에 발생한 RuntimeError는 실행기의 예산 가드에 해당해 호출 예산 소진으로 재집계했다."
        )
    if benchmark == "bird":
        time_note += (
            " 기준군 Luna D의 29·69·146·287번은 #276이 남긴 최종 재실행 결과를 사용했다. "
            "최초 중단 기록은 해당 조합의 `baseline/interruptions.jsonl`에 있다."
        )
    content = [] if pr_body else [f"# {NAME[benchmark]} 스키마 복구 평가 (#278)"]
    content.extend(
        [
            f"## 실행 조건",
            settings_header(benchmark, data, manifest, groups, prefix),
            f"## 주요 지표",
            comparative
            + f" 변경군 완료 {completed:,}건, 채점 {n_scored:,}건, API 오류 호출 {errors:,}회, 재실행 {reruns:,}문항이다.",
            "\n".join(
                f"- {g['group']} 정답 수 {g['baseline']['correct']}/{g['pairs']} → "
                f"{g['treatment']['correct']}/{g['pairs']}, 변화폭 {g['delta_percentage_points']:+.2f}%p"
                for g in groups
            ),
            accuracy(groups, prefix),
            "### 실행 상태",
            runs(groups, prefix),
            time_note
            + (" API 오류 유형은 " + api_errors + "다." if api_errors else ""),
            "### 최종 SQL 제출과 실행",
            submissions(groups, prefix),
            state_note,
            "### 스키마 복구",
            recovery(groups, prefix),
            recovery_note,
            "### 호출, 토큰, 문항 시간",
            calls(groups, prefix),
            call_note,
            f"## 문항별 상태 전환",
            "아래 표는 기준군의 최종 `no such column` 문항을 분모로 집계했다. "
            f"모든 시작 상태의 전환 행렬은 비교 JSON의 `state_transitions`에 있다. "
            f"[전체 전환 행렬]({prefix}comparison.json)",
            transitions(groups, prefix),
            f"## 상세 기록과 재현 명령",
            response_models(groups, prefix),
            links(groups, prefix),
            f"[전체 집계 JSON]({prefix}comparison.json) · [평가 입력 manifest]({prefix}manifest.json)",
            REPRO[benchmark],
        ]
    )
    published_repository = "jhpark324/Lang2SQL"
    published_path = (
        "bench/bird/results/schema-275-v1/paired-scores.jsonl"
        if benchmark == "bird"
        else "bench/spider2/results/schema-275-v1/paired-scores.jsonl"
    )
    published_ref = (
        "0fb4f34d86d9ea845c30e7b5d1f2952ef30f02f8"
        if benchmark == "bird"
        else "306f5ba0c30bdf30cce510e758c38e3d6d75722d"
    )
    content.extend(
        [
            "`BASELINE`은 저장된 #275 원본이며 아래에서는 보고서에 동봉한 JSONL을 사용한다. "
            "`PUBLISHED`는 기존 PR의 문항별 점수 파일 사본이다. "
            f"[게시 기준군 문항별 점수](https://github.com/{published_repository}/blob/{published_ref}/{published_path})",
            f"```bash\nBASELINE=bench/{benchmark}/results/schema-278-v1\n"
            f'PUBLISHED="$BASELINE/published-baseline-scores.jsonl"\n'
            f'"$PY" bench/report_schema_recovery.py {benchmark} --baseline "$BASELINE" \\\n'
            f'  --treatment "$RUN" --db-dir "$DB_DIR" --published "$PUBLISHED" \\\n'
            f'  --output "$RUN/comparison"\n```',
        ]
    )
    if pr_body:
        content.append(f"[평가 보고서](SCHEMA_RECOVERY_RESULTS.md)")
    return "\n\n".join(content) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("benchmark", choices=NAME)
    args = parser.parse_args()
    output = Path("bench") / args.benchmark
    results = output / "results/schema-278-v1"
    data, manifest = load(results / "comparison.json"), load(results / "manifest.json")
    (output / "SCHEMA_RECOVERY_RESULTS.md").write_text(
        render(args.benchmark, data, manifest), encoding="utf-8"
    )
    (output / "SCHEMA_RECOVERY_PR_BODY.md").write_text(
        render(args.benchmark, data, manifest, pr_body=True), encoding="utf-8"
    )


if __name__ == "__main__":
    main()
