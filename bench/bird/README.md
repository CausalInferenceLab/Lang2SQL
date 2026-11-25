# BIRD Mini-Dev SQLite 100문항 벤치

제품 변경 전후를 같은 문항으로 평가하는 모듈이다. 이 벤치의 커밋과 제품의 기준·변경 커밋을 각각 체크아웃하고, 같은 벤치 워크트리에서 두 제품을 평가한다.

## 고정 프로토콜

- [표본 manifest](manifest-sqlite100.json)는 기존 벤치와 동일한 SQLite 리비전에서 evidence가 있는 365문항을 대상으로 한다. `db_id`와 난이도별 비례 몫을 최대 나머지 방식으로 정하고, 각 그룹에서 `SHA-256(20261114:question_id)`가 작은 문항을 뽑았다. 고정한 100문항에 11개 DB와 `challenging` 98건, `moderate` 2건이 포함된다. [기존 결과 PR #315](https://github.com/CausalInferenceLab/Lang2SQL/pull/315)
- **B**는 질문과 같은 사용자 메시지에 `BIRD evidence` 블록으로 원문을 전달한다. **D**는 질문에 공통 SQL 제출 지시만 붙인다. 각 문항은 새 세션과 빈 지식 저장소에서 시작한다. 제품의 `run_sql`과 `explore_schema`를 사용하며, 해당 제품이 제공하는 `review_sql`도 노출한다.
- [프로토콜 설정](protocol.py)은 모델 ID, 응답 확인용 모델 ID, 호출 설정을 고정한다. `gpt-4.1-mini-2025-04-14`와 `gpt-4.1-2025-04-14`는 OpenAI SDK 3.51.0, `gpt-6-luna`는 SDK 3.57.0과 `reasoning_effort=none`을 사용한다. Luna는 공급자의 별칭으로 지정하고 응답 모델 ID와 제공되는 `system_fingerprint`를 기록한다. [기존 런타임 설정](https://github.com/CausalInferenceLab/Lang2SQL/pull/315)
- Python 3.13.14, SQLite 3.53.1, LiteLLM 1.109.1을 사용한다. SQLAlchemy와 나머지 패키지는 [잠금 파일](uv.lock)에 고정한다. 문항당 모델 호출은 8회, 출력은 호출당 4,096토큰으로 제한한다. 호출·문항·SQL 제한은 각각 120초·480초·30초이며, 채점 결과는 100,000행까지 읽는다. 각 모델은 B, D 순서로 실행하고 세 모델은 병렬로 돌린다. 조합당 동시성은 4다.
- EX는 읽기 전용 SQLite에서 예측과 기준 SQL의 행 튜플 집합이 같으면 1점이다. 행 순서와 중복은 무시하고, 열 순서와 값은 유지한다. SQL 미제출과 실행 실패도 분모에 포함한다. [BIRD EX 규칙](https://github.com/bird-bench/mini_dev/blob/1c49a375e76c471e218564da3f4617513d33e371/evaluation/evaluation_ex.py#L17)

같은 100문항으로 3개 프로필과 2개 입력 조건을 평가하므로 제품 버전당 600회다. 기준·변경군을 모두 새로 실행하면 1,200회다. 이미 같은 벤치와 설정으로 저장된 기준군 행렬이 있으면 `compare`에서 다시 사용할 수 있다.

## 워크트리와 입력 준비

절대 경로 `BENCH_TREE`, `BASE_TREE`, `CANDIDATE_TREE`, `RUN_ROOT`를 정한다. `BENCH_SHA`에 이 모듈의 전체 커밋 SHA를, `BASE_SHA`와 `CANDIDATE_SHA`에 제품 커밋의 전체 SHA를 지정한다. GitHub에서 벤치 PR 참조를 가져올 때는 `BENCH_PR`에 PR 번호를 넣는다.

```bash
git fetch origin "refs/pull/$BENCH_PR/head"
git worktree add --detach "$BENCH_TREE" "$BENCH_SHA"
git worktree add --detach "$BASE_TREE" "$BASE_SHA"
git worktree add --detach "$CANDIDATE_TREE" "$CANDIDATE_SHA"
cd "$BENCH_TREE"

mkdir -p "$RUN_ROOT"
UV_PROJECT_ENVIRONMENT="$RUN_ROOT/env-gpt41" \
  uv sync --project bench/bird --locked --managed-python --python 3.13.14 --extra gpt41
UV_PROJECT_ENVIRONMENT="$RUN_ROOT/env-luna" \
  uv sync --project bench/bird --locked --managed-python --python 3.13.14 --extra luna
export BIRD_GPT41_PY="$RUN_ROOT/env-gpt41/bin/python"
export BIRD_LUNA_PY="$RUN_ROOT/env-luna/bin/python"
```

`SOURCE`는 고정 리비전의 `data/mini_dev_sqlite.jsonl` 경로다. `DB_DIR`은 BIRD Dev 배포본의 내부 `dev_databases.zip`을 푼 디렉터리다. `<db_id>/<db_id>.sqlite`와 `<db_id>.sqlite` 구조를 지원한다. `prepare`가 주석과 DB 파일의 SHA-256을 manifest와 대조하고, 모델 입력용 `cases.jsonl`로 내보낸다. 기준 SQL은 별도의 채점 입력으로 사용한다. [고정 주석 리비전](https://huggingface.co/datasets/birdsql/bird_mini_dev/tree/38818fcbc46038129592680c51b2c8b597aac27c/data) · [SQLite 배포본](https://bird-bench.oss-cn-beijing.aliyuncs.com/dev.zip)

```bash
PREPARED="$RUN_ROOT/prepared"
"$BIRD_GPT41_PY" -m bench.bird.evaluate prepare \
  --source "$SOURCE" --db-dir "$DB_DIR" --output "$PREPARED"
"$BIRD_GPT41_PY" -m bench.bird.evaluate check-gold \
  --source "$SOURCE" --db-dir "$DB_DIR" --prepared "$PREPARED"
```

## 변경 전후 평가

실행 환경에 기존 벤치의 LiteLLM 인증을 제공한 뒤 아래 명령을 실행한다. `matrix.sh`에서 같은 벤치 워크트리와 준비 입력으로 각 제품의 6개 조합을 생성·채점한다. 동일한 명령을 다시 실행하면 기록이 있는 조합부터 이어서 진행한다.

```bash
bash bench/bird/matrix.sh "$BASE_TREE" "$BASE_SHA" \
  "$PREPARED" "$SOURCE" "$DB_DIR" "$RUN_ROOT/baseline"
bash bench/bird/matrix.sh "$CANDIDATE_TREE" "$CANDIDATE_SHA" \
  "$PREPARED" "$SOURCE" "$DB_DIR" "$RUN_ROOT/candidate"
"$BIRD_GPT41_PY" -m bench.bird.evaluate compare \
  --baseline "$RUN_ROOT/baseline" --candidate "$RUN_ROOT/candidate" \
  --output "$RUN_ROOT/comparison"
```

각 조합의 `settings.json`은 두 커밋, 데이터·잠금 파일 지문, 모델 프로필과 실제 런타임을 기록한다. `compare`는 동일한 벤치와 입력, 프로필별 런타임으로 완료한 조합만 받아들인다. `comparison.json`에서 조건·프로필별 제품 전후 EX, 정오답 전환, 호출 수와 모델 응답 정보를 확인할 수 있다. 제품별 B−D도 함께 계산한다. `paired-scores.jsonl`에는 문항 ID별 쌍을 저장한다.

## 오프라인 스모크

`prepare --smoke-cases 2`로 다른 DB에서 고정 문항 두 개를 준비하고 `check-gold`를 실행한다. 위의 두 `matrix.sh` 호출에 `--smoke`를 추가하고, `compare`에도 `--smoke`를 붙인다. 별도 결과 경로를 사용한다. 스크립트 응답으로 실제 제품의 스키마 조회와 SQLite 실행, 최종 SQL 제출, EX 채점, 문항 대응을 확인한다. 오프라인 결과에는 `scripted-smoke` 모드가 기록된다.
