# BIRD Mini-Dev SQLite에서 evidence가 제공된 365문항

[실측 결과와 재현 절차](RESULTS.md)에서 2026년 11월 22일 평가를 확인할 수 있다.

고정한 제품 소스에서 조건과 모델 프로필을 선택해 365문항을 평가한다.
한 실행은 조건 하나와 모델 프로필 하나에 해당하며 문항당 한 번 응답한다.

| 조건 | 사용자 메시지 | 첫 요청 전 지식 KV |
|---|---|---|
| A | 질문 | `term_custom`의 `team` 범위에 evidence 원문 1건 |
| D | 질문 | 비어 있음 |
| B | 질문과 evidence 원문 | 비어 있음 |

A의 시스템 메시지에는 제품이 렌더링한 용어명과 범위 표기가 포함된다.
프롬프트에서 사용하는 모델 역할과 도구는 제품에서 가져온다.
벤치마크는 세 조건에 공통으로 최종 SQL 지시를 붙이고, 새 세션과 지식 KV에서
`run_sql`, `explore_schema`만 제공한다. 준비한 문항별 evidence를 처음부터
알고 있는 한 턴의 처리 성능을 측정한다.

## 입력과 환경

- 주석은 [BIRD Mini-Dev 2026 SQLite](https://huggingface.co/datasets/birdsql/bird_mini_dev/commit/38818fcbc46038129592680c51b2c8b597aac27c)의
  `data/mini_dev_sqlite.jsonl`이다. [manifest-sqlite365.json](manifest-sqlite365.json)에
  원본 SHA-256과 evidence가 비어 있지 않은 365개 ID를 고정한다.
- [BIRD의 SQLite 배포본](https://hub.docker.com/r/3036197201/bird-mini-dev-sqlite)에서
  11개 DB를 확보하고 이미지 digest 또는 해당 아카이브 해시를 `--db-revision`에
  지정한다. `--db-dir` 안에 `<db_id>.sqlite` 또는 `<db_id>/<db_id>.sqlite` 구조로
  둔다. 준비 후 각 DB의 실제 바이트 해시로 식별한다.
- 기본 제품 기준은 `e0a7fb1`이다. 해당 평가를 재현할 때는 [실측에 사용한 커밋](RESULTS.md#재현)을
  체크아웃한다. 다른 제품 버전을 평가할 때는 `run --product-commit HEAD`처럼
  제품 커밋을 지정한다. 실행기는 현재 `src/`와 지정한 커밋을 대조하고 SHA를
  `settings.json`에 저장한다. PR [#260](https://github.com/CausalInferenceLab/Lang2SQL/pull/260)의
  LiteLLM 계측, 단일 SQL 추출 및 읽기 전용 SQLite 구성을 `bench/common/`에
  가져왔다. `benchmark` extra로 #260의 LiteLLM 버전을 설치한다.

## 명령 순서

아래의 데이터 파일, DB 경로, DB 리비전을 준비한 뒤 저장소 루트에서 실행한다.
`run`은 `--condition`과 `--profile`로 실행할 조합을 받는다. 기본
[모델 프로필 파일](models.example.json)은 [PR #260의 프로필](https://github.com/CausalInferenceLab/Lang2SQL/blob/24cff8cb1126fc017d1c68389b76213858e3a2e6/bench/spider2/models.example.json)에
`openai-gpt6-luna`를 추가한 구성이다. `--profiles`로 같은 형식의 다른 JSON을
지정할 수 있다.
실제 모델을 실행할 때는 선택한 프로필의 `api_key_env` 환경 변수를 사용한다.
Luna 프로필은 Chat Completions에서 함수 호출을 지원하는 모드인
`reasoning_effort`의 `none`을 지정한다. [OpenAI Luna 모델 문서](https://developers.openai.com/api/docs/models/gpt-6-luna)

```bash
uv sync --locked --extra benchmark

SOURCE=/absolute/path/to/mini_dev_sqlite.jsonl
DB_DIR=/absolute/path/to/sqlite-snapshots
DB_REVISION='<sqlite-image-digest-or-archive-sha256>'
PREPARED=bench/bird/runs/prepared-sqlite365
BIRD_CONDITION=B
BIRD_PROFILE=openai-gpt41-mini
DRY_RUN=bench/bird/runs/check-${BIRD_CONDITION}-${BIRD_PROFILE}
RUN=bench/bird/runs/sqlite365-${BIRD_CONDITION}-${BIRD_PROFILE}-v1

uv run --locked --extra benchmark python -m bench.bird.evaluate prepare \
  --source "$SOURCE" --db-dir "$DB_DIR" --db-revision "$DB_REVISION" \
  --output "$PREPARED"
uv run --locked --extra benchmark python -m bench.bird.evaluate run \
  --condition "$BIRD_CONDITION" --profile "$BIRD_PROFILE" \
  --dry-run --prepared "$PREPARED" --db-dir "$DB_DIR" --output "$DRY_RUN"
uv run --locked --extra benchmark python -m bench.bird.evaluate score \
  --gold-only --source "$SOURCE" --db-dir "$DB_DIR" --output "$PREPARED"
uv run --locked --extra benchmark python -m bench.bird.evaluate run \
  --condition "$BIRD_CONDITION" --profile "$BIRD_PROFILE" \
  --prepared "$PREPARED" --db-dir "$DB_DIR" --output "$RUN"
uv run --locked --extra benchmark python -m bench.bird.evaluate score \
  --source "$SOURCE" --db-dir "$DB_DIR" --output "$RUN"
```

`prepare`는 gold SQL을 빼고 문항 정보를 `cases.jsonl`로 내보낸다. `score
--gold-only`는 정답 SQL 전수를 실제 배포 DB와 예산으로 실행하고 통과 표식을
남긴다. 정답 파일과 gold 결과는 모델 실행 입력에 들어가지 않는다.
`run --dry-run`은 API 키 없이 선택한 조건의 첫 모델 요청과 KV 격리를 점검한다.
모의 응답으로 생성한 디렉터리는 프롬프트 검사용으로 표시한다.

`run`은 manifest에 기록된 365문항을 고정 seed `20261114`로 섞는다.
첫 6개 문항에서 선택한 모델의 응답과 저장 상태를 점검한 뒤 나머지 문항을
실행한다. 문항당 모델 호출은 총 8회까지 허용한다. 호출당 출력 4,096토큰,
API 120초, 문항 480초, SQL 30초, 최종 조회 100,000행, 동시 실행 4건을
적용한다. 공급자가 반환한 모델 ID와 토큰 사용량은 각 호출에 기록한다.

중단 후에는 같은 `run` 명령에 `--resume`을 붙인다. 완료된 파일은 그대로
사용하고, 강제 종료 전에 모델 요청을 시작했다는 표식만 남은 문항은
`interrupted`로 기록해 EX 0점으로 집계한다. 정상 종료 신호를 받으면 새
작업 배정을 멈추고 진행 중인 문항 결과를 저장한다. 프로토콜 또는
모델 설정 오류로 차단된 실행은 원인을 해결한 뒤 새 디렉터리에서 시작한다.

## 채점과 결과

채점기는 최종 답변의 SQL 블록 하나를 SQLite 읽기 전용 연결에서 실행한다.
최종 SQL과 gold의 원본 행 튜플 집합이 같으면 EX 1이다. 행 순서와 중복은
무시하고 각 튜플의 열 위치와 값은 유지한다. 생성 오류, 빈 제출, 예측 SQL
오류, 타임아웃, 행 상한 초과도 분모에 포함한다. 비교 규칙은 고정된
[BIRD `calculate_ex`](https://github.com/bird-bench/mini_dev/blob/1c49a375e76c471e218564da3f4617513d33e371/evaluation/evaluation_ex.py#L17)와
같으며 실행 제한과 분모 관리는 이 평가의 프로토콜에 따른다.

- `manifest.json`, `settings.json`은 입력과 실행 코드를 재현할 수 있는 지문을
  저장한다. `settings.json`에는 선택한 조건과 모델 프로필이 들어간다.
  `status.json`은 생성 단계의 완료 상태를 기록한다.
- `records/<id>.json`에는 실제 첫 모델 요청의
  시스템·사용자 메시지, evidence 위치, 도구 요청·응답, 최종 SQL, 실패 사유와
  호출 사용량이 남는다. `sql/<id>.sql`과 `scores.json`은 채점 단계에서 생성된다.
- `summary.json`에는 이 실행의 EX, 실패 사유, 호출과 토큰 집계가 들어간다.
  조건이나 모델을 비교할 때는 `manifest_digest`가 같은 결과를 골라
  `scores.json`의 `question_id`로 연결할 수 있다.

실행 디렉터리는 로컬에 보관한다. 결과를 공유할 때는 설정과 SQL, 점수 및
요약 파일을 사용한다. BIRD 원본은 공식 데이터 페이지의 이용 조건을 따른다.
