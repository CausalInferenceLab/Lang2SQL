# BIRD Mini-Dev SQLite SQL 실행 후 답변 검증 평가 (#313)

BIRD 문항 대응·완료·채점은 각각 3,285/3,285건입니다. 전체 EX는 836/3,285 (25.45%)에서 820/3,285 (24.96%)로 -0.49%p 변했습니다. 모델 호출은 13,565회에서 13,700회, API 오류 호출은 1회에서 0회였습니다. 변경군 중단 기록은 0건입니다.

## 실행 정확도

| 모델 | #279 EX | #314 EX | 변화 (%p) | 정답→오답 | 오답→정답 |
| --- | ---: | ---: | ---: | ---: | ---: |
| GPT‑4.1 mini | 195/1,095 (17.81%) | 196/1,095 (17.90%) | +0.09 | 77 | 78 |
| GPT‑4.1 | 223/1,095 (20.37%) | 197/1,095 (17.99%) | -2.37 | 88 | 62 |
| GPT‑6 Luna | 418/1,095 (38.17%) | 427/1,095 (39.00%) | +0.82 | 106 | 115 |
| 합산 | 836/3,285 (25.45%) | 820/3,285 (24.96%) | -0.49 | 271 | 255 |

| 조건 · 모델 | #279 EX | #314 EX | 변화 (%p) | 정답→오답 | 오답→정답 |
| --- | ---: | ---: | ---: | ---: | ---: |
| [A · GPT‑4.1 mini](results/post-sql-answer-check-313-v1/A-openai-gpt41-mini/paired-scores.jsonl) | 69/365 (18.90%) | 68/365 (18.63%) | -0.27 | 25 | 24 |
| [A · GPT‑4.1](results/post-sql-answer-check-313-v1/A-openai-gpt41/paired-scores.jsonl) | 80/365 (21.92%) | 66/365 (18.08%) | -3.84 | 30 | 16 |
| [A · GPT‑6 Luna](results/post-sql-answer-check-313-v1/A-openai-gpt6-luna/paired-scores.jsonl) | 149/365 (40.82%) | 154/365 (42.19%) | +1.37 | 39 | 44 |
| [B · GPT‑4.1 mini](results/post-sql-answer-check-313-v1/B-openai-gpt41-mini/paired-scores.jsonl) | 74/365 (20.27%) | 76/365 (20.82%) | +0.55 | 29 | 31 |
| [B · GPT‑4.1](results/post-sql-answer-check-313-v1/B-openai-gpt41/paired-scores.jsonl) | 79/365 (21.64%) | 75/365 (20.55%) | -1.10 | 29 | 25 |
| [B · GPT‑6 Luna](results/post-sql-answer-check-313-v1/B-openai-gpt6-luna/paired-scores.jsonl) | 146/365 (40.00%) | 144/365 (39.45%) | -0.55 | 42 | 40 |
| [D · GPT‑4.1 mini](results/post-sql-answer-check-313-v1/D-openai-gpt41-mini/paired-scores.jsonl) | 52/365 (14.25%) | 52/365 (14.25%) | +0.00 | 23 | 23 |
| [D · GPT‑4.1](results/post-sql-answer-check-313-v1/D-openai-gpt41/paired-scores.jsonl) | 64/365 (17.53%) | 56/365 (15.34%) | -2.19 | 29 | 21 |
| [D · GPT‑6 Luna](results/post-sql-answer-check-313-v1/D-openai-gpt6-luna/paired-scores.jsonl) | 123/365 (33.70%) | 129/365 (35.34%) | +1.64 | 25 | 31 |

## 실행 지표

화살표는 기준군 #279 → 변경군 #314입니다.

| 조건 · 모델 | SQL 미제출 | 모델 호출 | 변경 재실행 문항 | 변경 재실행 횟수 | 기준 불일치 회복 |
| --- | ---: | ---: | ---: | ---: | ---: |
| A · GPT‑4.1 mini | 28 → 27 | 1,688 → 1,640 | 45 → 35 | 120 → 89 | 15/229 |
| A · GPT‑4.1 | 2 → 4 | 1,465 → 1,460 | 2 → 2 | 8 → 3 | 10/202 |
| A · GPT‑6 Luna | 19 → 11 | 1,348 → 1,384 | 76 → 103 | 219 → 225 | 38/197 |
| B · GPT‑4.1 mini | 34 → 29 | 1,695 → 1,681 | 40 → 47 | 93 → 103 | 23/231 |
| B · GPT‑4.1 | 2 → 11 | 1,473 → 1,532 | 2 → 0 | 3 → 0 | 18/211 |
| B · GPT‑6 Luna | 14 → 22 | 1,339 → 1,423 | 78 → 125 | 204 → 273 | 38/204 |
| D · GPT‑4.1 mini | 38 → 30 | 1,703 → 1,661 | 39 → 43 | 105 → 106 | 18/236 |
| D · GPT‑4.1 | 2 → 3 | 1,431 → 1,416 | 4 → 0 | 7 → 0 | 16/208 |
| D · GPT‑6 Luna | 26 → 30 | 1,423 → 1,503 | 95 → 125 | 263 → 328 | 27/216 |

| 모델 | SQL 미제출 | 8회 소진 미제출 | `run_sql` 성공 문항 | 입력 토큰 | 출력 토큰 |
| --- | ---: | ---: | ---: | ---: | ---: |
| GPT‑4.1 mini | 100 → 86 | 98 → 80 | 952 → 958 | 7,111,766 → 7,277,375 | 805,015 → 795,688 |
| GPT‑4.1 | 6 → 18 | 6 → 18 | 802 → 784 | 5,151,474 → 5,565,939 | 610,755 → 617,868 |
| GPT‑6 Luna | 59 → 63 | 59 → 63 | 1,094 → 1,095 | 6,009,998 → 6,988,160 | 492,802 → 542,698 |

변경 재실행은 이전 모델 호출들에서 마지막으로 성공한 `run_sql`의 SQL과 새로 SQLite 실행에 도달한 SQL의 토큰이 다른 경우입니다. 공백, 주석, 따옴표 밖 ASCII 대소문자, 끝의 세미콜론을 제외합니다. 조건에 해당한 문항 수와 실행 시도 수를 각각 집계했습니다. 기준 불일치 회복의 분모는 #279에서 `run_sql` 성공 응답이 있었으며 최종 SQL의 행 집합이 달랐던 문항 수입니다. 분자는 그중 #314에서 EX 정답이 된 문항 수입니다.

## 다음 모델 턴 확인

GPT‑6 Luna에 고정된 첫 조회 결과를 전달한 세 사례를 확인했습니다. 올바른 집계값 0과 반환 행 0개를 구분했습니다. 구매일로 집계한 사례에서는 첫 SQL을 그대로 인용해 0건으로 답했습니다. 배송 완료일 기준 정답은 2건입니다. [실행 기록](results/post-sql-answer-check-313-v1/prompt-probes.json)

## 비교 조건과 실행 기록

기준군은 #280에 게시된 #279의 `fb1f36e9b0980c5e593d2fddb5ecc6607cd101d7` 실행입니다. 변경군은 #314의 `f4635ea567698f32d788688d327d2af33b59de9b`를 체크아웃한 워크트리에서 실행했습니다. 실행기 소스와 제품 잠금 파일을 #280 설정의 SHA‑256과 대조했습니다. 문항별 첫 요청에서 추가한 프롬프트 문단을 제거하면 시스템 메시지가 기준군과 일치했고 사용자 메시지와 도구 스키마도 3,285건 모두 일치했습니다. [기준군 원본](https://github.com/jhpark324/Lang2SQL/tree/f47da5c669a2c691d59d90fd236e227f5db0da96/bench/bird/results/schema-278-v1) · [제품 PR #314](https://github.com/CausalInferenceLab/Lang2SQL/pull/314)

변경군 생성 시작부터 마지막 채점 파일 저장까지 UTC `2026-11-25T07:55:35.199911+00:00` ~ `2026-11-25T08:48:34.604646+00:00`입니다. [실행별 해시와 시각](results/post-sql-answer-check-313-v1/provenance.json)

문항은 evidence가 있는 BIRD Mini-Dev SQLite 365개와 동일한 DB 11개입니다. A는 `term_custom` 팀 레이어에 evidence를 등록하고 B는 질문에 첨부하며 D는 질문만 전달합니다. 각 문항은 새 세션으로 시작하며 최종 답변으로 SQL 블록 하나를 제출합니다. EX는 SQLite에서 예측과 기준 SQL을 실행해 반환한 행 튜플 집합이 같으면 1점입니다. 행 순서와 중복을 무시하며 미제출과 실행 실패도 분모에 포함했습니다. [BIRD EX 채점 규칙](https://github.com/bird-bench/mini_dev/blob/1c49a375e76c471e218564da3f4617513d33e371/evaluation/evaluation_ex.py#L17)

데이터 리비전은 `38818fcbc46038129592680c51b2c8b597aac27c`입니다. manifest SHA‑256은 `05d1414e7a355c486331506a8a9619428add7df6beae76789f8ce6fa647edb5b`이며 DB별 해시를 확인하고 기준 SQL 365개를 모두 실행했습니다. [Manifest](results/post-sql-answer-check-313-v1/manifest.json) · [기준 SQL 검사](results/post-sql-answer-check-313-v1/gold-check.json)

| 모델 | 요청 모델 ID | 기준 · 변경 응답 모델 ID | OpenAI SDK |
| --- | --- | --- | --- |
| GPT‑4.1 mini | `openai/gpt-4.1-mini-2025-04-14` | `gpt-4.1-mini-2025-04-14` · `gpt-4.1-mini-2025-04-14` | 3.51.0 |
| GPT‑4.1 | `openai/gpt-4.1-2025-04-14` | `gpt-4.1-2025-04-14` · `gpt-4.1-2025-04-14` | 3.51.0 |
| GPT‑6 Luna | `openai/gpt-6-luna` · `reasoning_effort=none` | `gpt-6-luna` · `gpt-6-luna` | 3.57.0 |

Python 3.13.14, SQLite 3.53.1, LiteLLM 1.109.1, SQLAlchemy 2.0.50을 사용했습니다. 문항당 모델 호출 8회, 호출당 최대 출력 4,096토큰, 호출·문항·SQL 제한 120·480·30초, 채점 결과 최대 100,000행, 공급자 자동 재시도 0회를 적용했습니다. 동시성은 조합당 4이며 세 모델을 병렬로 실행했습니다. 각 모델은 A·B·D 순서로 실행했습니다.

기준군은 #280의 저장된 단일 실행이고 변경군은 이번 단일 실행입니다. 측정 시점과 모델 응답 변동이 관측 차이에 함께 반영될 수 있습니다. GPT‑6 Luna의 반환 모델 ID는 `gpt-6-luna` 별칭입니다.

각 조합의 `records.jsonl.gz`에 최초 요청, 응답 모델 ID, 도구의 SQL과 반환 결과를 담았습니다. `scores.json`과 `paired-scores.jsonl`은 문항 ID와 기록 해시로 원본과 연결됩니다. [전체 집계 JSON](results/post-sql-answer-check-313-v1/comparison.json) · [집계 스크립트](post_sql_answer_check_report.py)

## 재현

`SOURCE`에는 고정 SQLite JSONL 주석을, `DB_DIR`에는 11개 DB 스냅샷을 지정합니다. `BASE_TREE`, `PRODUCT_TREE`, `DEPS`, `SDK_351`, `RUN`은 새 작업 경로입니다. `CREDENTIALS`는 기존 로컬 모델 설정 파일의 경로입니다. [#280 입력과 실행 환경](https://github.com/CausalInferenceLab/Lang2SQL/pull/280)

```bash
git fetch https://github.com/jhpark324/Lang2SQL.git \
  feature/313-post-sql-answer-check experiment/278-bird-schema-recovery experiment/bird-sqlite
PRODUCT=f4635ea567698f32d788688d327d2af33b59de9b
EVALUATOR=781fd3871a87afd689794204343b0d9e9a0eb601
git worktree add --detach "$BASE_TREE" f47da5c669a2c691d59d90fd236e227f5db0da96
git worktree add --detach "$PRODUCT_TREE" "$PRODUCT"
git archive "$EVALUATOR" bench/bird bench/common | tar -x -C "$PRODUCT_TREE"
git worktree add --detach "$DEPS" 1f47f2821559992717aeb23e1cd34ca489142300
uv sync --project "$DEPS" --locked --extra benchmark --python 3.13.14
PY="$DEPS/.venv/bin/python"
uv pip install --python "$PY" --target "$SDK_351" --no-deps openai==3.51.0

cd "$PRODUCT_TREE"
export PYTHONPATH="$PWD/src:$PWD"
export LITELLM_LOCAL_MODEL_COST_MAP=True LITELLM_LOG=ERROR
git diff --exit-code "$PRODUCT" -- src/
"$PY" -m bench.bird.evaluate prepare --source "$SOURCE" --db-dir "$DB_DIR" \
  --db-revision 8132aacfe61ecec7198a62e93cb8512dac89b1d6b9922fe3c7bb94b13f317d8f \
  --output "$RUN/prepared"
BASELINE="$BASE_TREE/bench/bird/results/schema-278-v1"
cmp "$RUN/prepared/manifest.json" "$BASELINE/manifest.json"
"$PY" -m bench.bird.evaluate score --gold-only --source "$SOURCE" \
  --db-dir "$DB_DIR" --output "$RUN/prepared"

lane() (
  set -e
  PROFILE=$1
  if [ "$PROFILE" != openai-gpt6-luna ]; then
    export PYTHONPATH="$PYTHONPATH:$SDK_351"
  fi
  for CONDITION in A B D; do
    OUT="$RUN/$CONDITION-$PROFILE"
    "$PY" -m bench.bird.evaluate run --condition "$CONDITION" --profile "$PROFILE" \
      --prepared "$RUN/prepared" --db-dir "$DB_DIR" --env-file "$CREDENTIALS" \
      --product-commit "$PRODUCT" --output "$OUT"
    "$PY" -m bench.bird.evaluate score --source "$SOURCE" --db-dir "$DB_DIR" \
      --output "$OUT"
  done
)
lane openai-gpt41-mini & MINI_PID=$!
lane openai-gpt41 & FULL_PID=$!
lane openai-gpt6-luna & LUNA_PID=$!
wait "$MINI_PID"
wait "$FULL_PID"
wait "$LUNA_PID"
```

중단된 생성을 이어갈 때는 같은 실행의 `run`에 `--resume`을 붙입니다. 생성이 완료된 실행은 `score`부터 재개합니다.

아래 문항 대응 집계는 게시된 기록에도 적용할 수 있습니다. `RESULTS_TREE`는 결과 PR 워크트리이며 `--treatment`에 `"$RUN"` 또는 `"$RESULTS_TREE/bench/bird/results/post-sql-answer-check-313-v1"`을 사용할 수 있습니다.

```bash
python "$RESULTS_TREE/bench/bird/post_sql_answer_check_report.py" \
  --baseline "$BASELINE" --treatment "$RUN" --output "$RUN/comparison"
```
