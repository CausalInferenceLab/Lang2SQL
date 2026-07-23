# Lang2SQL Discord 사용 가이드

업무 의미 검토형 질의 모드에서 LLM은 SQL을 직접 쓰지 않는다. LLM은 질문을 서버가 검증할
수 있는 지표·집계·분류 슬롯으로 조립하고, 서버가 확인된 값만으로 read-only SQL을
만든다.

## 준비

실제 질문에는 tool calling이 가능한 모델이 필요하다. OpenAI를 사용하거나 Ollama
같은 OpenAI-compatible 서버를 연결한다.

```bash
uv sync
# Discord /setup에서 DuckDB를 선택할 경우
uv sync --extra duckdb
```

```bash
export LANG2SQL_LLM_BASE_URL=http://127.0.0.1:11434
export LANG2SQL_LLM_MODEL=gemma4:26b
export DISCORD_BOT_TOKEN=...

# 선택: 일반 구성원이 질문할 수 있는 Discord 상위 채널 ID만 쉼표로 지정한다.
# 비워 두면 서버 관리자의 질문만 허용된다. thread는 상위 채널 정책을 따른다.
export LANG2SQL_DISCORD_QUERY_CHANNEL_IDS=123456789012345678

.venv/bin/lang2sql-bot
```

모델 설정이 없으면 `FakeLLM`이 사용된다. 이는 설치 확인용일 뿐 자연어 질의
검증용이 아니다. 채널 허용 목록은 Discord 접근 경계일 뿐 DB 자체의 row/column
권한을 대신하지 않는다.

> Discord가 아닌 서비스에 내장할 때는 `/setup`이나 `semantic_query` 대신 SQL 없는
> `Lang2SQLRuntime`을 쓴다. 공개 흐름은 `connect → candidates → human feedback →
> typed plan → execute`이며, 연결 검토의 실제 선택과 실행 결과 type-check를 포함한
> 예제는 [`LIBRARY_API.md`](LIBRARY_API.md)에 있다.

## 1. `/setup`으로 DB 연결

Discord 서버 관리자가 `/setup`을 실행하고 DB 종류와 접속 정보를 입력한다.
지원 선택지는 SQLite, PostgreSQL, MySQL, Snowflake, BigQuery, DuckDB, D1이다.
credential-bearing DSN을 채널 명령으로 직접 받는 `/connect`는 노출하지 않는다.

처음 로컬 검증은 SQLite가 가장 단순하다.

1. `/setup`
2. `SQLite` 선택
3. 봇 프로세스에서 접근 가능한 DB 파일의 절대 경로 입력
4. `/semantic_status`로 연결과 검토 대기 상태 확인

연결 성공 메시지는 테이블 수, 선언 FK 수, 민감·식별자·비지원 컬럼 차단 수와
관리자 공개 검토가 필요한 문자열 차원 수를 보여준다. 연결 단계에서는 업무
지표를 전부 묻지 않는다.

`/setup`이 catalog를 활성화한 뒤에는 같은 연결에서 다음 경계가 적용된다.

| 기능 | Catalog 없는 legacy mode | Catalog 있는 업무 의미 검토형 질의 모드 |
|---|---|---|
| 자연어 질의 도구 | 모델이 `run_sql`을 호출 | 모델은 `semantic_query`의 typed slot만 조립 |
| SQL 작성 | 모델이 SQL 문자열 작성 | 서버 compiler만 검증된 SQL 작성 |
| `/enrich`, `/org_setup` 샘플링 | 사용 가능 | raw-value sampling 비활성화 |
| `/ingest`, `/term_custom`, `/remember`, `/audit_me` | 사용 가능 | 기존 기능으로 유지 |
| Catalog 손상 | 해당 없음 | raw SQL로 돌아가지 않고 연결 차단 |

### 불투명한 분류 컬럼에 업무 표현 연결

`col_a`, `code_17`처럼 물리 이름만으로 업무 분류 의미를 알 수 없다면 관리자가
값을 보지 않고 표현만 연결할 수 있다.

1. `/semantic_dimension_candidates search:<물리 컬럼 이름>`
2. 결과의 15분 `mapping_token`과 분류 근거 확인
3. `/semantic_dimension_map candidate_token:<mapping_token> phrase:<업무 표현> confirm:false`
4. 같은 관리자·토큰·표현으로 `confirm:true`

이 경로는 모든 비차단 dimension을 탐색하지만 값 샘플을 읽지 않고 그룹 값 공개도
승인하지 않는다. 동일 표현이 다른 dimension에 이미 연결됐거나 이전 검토에서
거절됐으면 차단한다. 이후 질문에서 reviewed phrase가 shortlist 근거로 재사용된다.

### 문자열 분류값 공개 승인

질문에 필요한 문자열 차원이 공개 검토 대기라면 다음 순서를 따른다.

1. `/semantic_candidates search:<물리 컬럼 이름>`
2. 결과의 15분 `candidate_token`과 분류 근거 확인
3. `/semantic_release candidate_token:<토큰> disclosure_tier:controlled_grouped confirm:false`
4. 경고를 읽고 같은 관리자·토큰·등급으로 `confirm:true`

`controlled_grouped`는 각 결과 그룹의 실제 지표 기여 행이 5개 미만이면 전체
결과를 차단한다. `public_grouped`가 필요하면 먼저 아래 명령으로 연결 전체가
공개·비개인 데이터임을 확인해야 한다.

1. `/semantic_public_data enable:true confirm:false`
2. 경고에 표시된 `action_token`으로 권한 있는 관리자가 `confirm:true`
3. `/semantic_release ... disclosure_tier:public_grouped confirm:false`
4. 같은 관리자·토큰·등급으로 `confirm:true`

일부 컬럼만 공개이거나 개인·조직 민감 데이터가 섞였으면 전체 공개 확인을 쓰지
않는다. 공개 승인은 결과 라벨 표시 권한이고, 아래 질문 표현의 의미 연결과는
서로 대체되지 않는다. 두 흐름 모두 DB 값을 후보 화면이나 LLM에 샘플링하지 않는다.

### 불투명한 수치 컬럼에 업무 표현 연결

물리 이름만으로 지표를 좁힐 수 없다면 관리자가 다음 순서로 표현만 연결한다.

1. `/semantic_metric_candidates search:<물리 컬럼 이름>`
2. 결과의 15분 `candidate_token` 확인
3. `/semantic_metric_map candidate_token:<토큰> phrase:<업무 표현> confirm:false`
4. 같은 관리자·토큰·표현으로 `confirm:true`

이 명령은 표현과 수치 컬럼만 연결한다. `SUM`/`AVG` 같은 집계 의미는 실제 질문의
`/semantic_review`에서 별도로 확인한다.

## 2. 봇을 멘션해 질문

```text
@Lang2SQL Amount by region name
```

명시적 `@Lang2SQL` 멘션이 필요하다. 봇은 일반 대화, `@everyone`, `@here`를
질의로 가로채지 않는다. DM은 사용자별로 격리되어 허용된다. Discord 서버에서는
관리자 또는 `LANG2SQL_DISCORD_QUERY_CHANNEL_IDS`에 등록된 상위 채널의 구성원만
질문할 수 있다.

처음 보는 질문은 지표와 분류 표현이 독립 검토로 나뉠 수 있다. 예를 들어 지표
집계를 먼저 확인한 뒤, 같은 질문의 분류 표현 연결을 한 번 더 확인할 수 있다.
관리자는 `/semantic_reviews`에서 정확한 `review_id`를 복사하고 다음처럼 처리한다.

```text
/semantic_review review_id:<ID> aggregate:sum
/semantic_review review_id:<다음 ID> aggregate:confirm
```

대기열에는 원 질문·필터 값·날짜 경계를 저장하지 않는다. 봇 프로세스가 그대로면
승인 후 같은 typed draft를 자동 재개하고, 재시작된 경우에는 승인은 저장하되 원
요청자에게 같은 질문을 다시 보내도록 안내한다.

선택 가능한 값은 해당 검토 항목에 따라 `sum`, `avg`, `min`, `max`, `count`,
`confirm`, `reject` 중 일부다. 숫자 컬럼에는 `SUM`/`AVG`/`MIN`/`MAX`가 쓰이고,
`COUNT`는 물리 테이블의 source-record count에만 쓰인다.

승인에는 당시의 질문·연결 세대·catalog revision·지표·집계·분류가 함께 묶인다.
원 요청자가 직접 승인한 경우에는 원래 질문을 LLM 재해석 없이 재개한다. 관리자가
다른 사용자의 검토를 승인한 경우에는 연결만 저장하고 관리자 채널에서 DB 결과를
실행하거나 표시하지 않는다. 원 요청자가 같은 질문을 다시 보내야 한다.

## 3. 상태, 철회, 초기화

- `/semantic_status`: 자동 구조와 확인된 표현·집계·공개 정책 상태 확인
- `/semantic_candidates`: 문자열 차원 후보 검색 및 15분 토큰 발급(관리자)
- `/semantic_dimension_candidates`: 모든 비차단 분류 차원과 mapping token 검색(관리자)
- `/semantic_dimension_map`: 불투명한 분류 컬럼에 업무 표현만 연결(관리자)
- `/semantic_metric_candidates`: 수치 지표 후보 검색 및 15분 토큰 발급(관리자)
- `/semantic_reviews`: 현재 연결의 의미 검토 대기열(관리자)
- `/semantic_candidates state:released search:...`의 별도 `revoke_token`을 복사해
  `/semantic_revoke candidate_token:... confirm:false` → 같은 토큰으로 `confirm:true`
- `/semantic_public_data enable:false confirm:false` → 발급된 `action_token`으로 `confirm:true`
- `/semantic_reset confirm:false` → 발급된 `action_token`으로 `confirm:true`

모든 후보·행동 토큰은 15분 동안 source, 연결 세대, 행동 종류에 묶인다. 객체 후보
토큰은 관련 metric/dimension 상태와 epoch를, catalog-wide public/reset 토큰은 전체
catalog revision을 검증한다. metric/dimension map과 dimension release는 경고를 실행한 동일
관리자와 정확한 표현 또는 등급에도 추가로 묶인다. 연결이나 관련 검토 상태가
바뀌었거나 토큰이 만료되면 후보 목록 또는 경고 단계부터 다시 시작한다.
`/semantic_reset`은 사람이 확인한 표현·집계 연결, 문자열 공개 승인, 공개 데이터
범위를 함께 초기화하지만 물리 PK/FK와 기본 차단 정책은 제거하지 않는다.
실행 중 revoke/reset/재연결이 발생하면 실행 후, audit 후, Discord 렌더 직전의
catalog stamp 재검사에서 준비된 결과를 폐기한다.

## 현재 질의 및 공개 정책

지원:

- 숫자 컬럼의 표현별 `SUM`/`AVG`; `MIN`/`MAX`는 공개 데이터 범위에서만 지원
- 모든 테이블의 명시적 physical source-record `COUNT(*)` (PK 불필요)
- categorical group-by
- 선언 FK의 유일한 child-to-parent 1~N hop join
- nullable FK나 orphan fact를 보존하는 `LEFT JOIN`
- 명시적 AND 최대 8개의 exact `EQ` 또는 값 최대 20개의 `IN` bound filter
- native `DATE` 차원의 ISO date `[start, end)` 기간창
- 기존 file-backed SQLite와 DuckDB의 read-only governed execution

비공개 기본값에서는 그룹 유무와 무관하게 `SUM`/`AVG`/source-record `COUNT`의
실제 기여 행이 5개 미만이면 결과 전체를 차단하고, 단일 극값을 드러내는
`MIN`/`MAX`는 실행하지 않는다. 공개 데이터 범위에서도 하나라도
`controlled_grouped` 차원을 사용하면 이 보호가 유지된다. `public_grouped`는 최소
그룹 보호를 해제하지만 최대 50범주와 라벨 128자 제한은 유지한다. 이 숫자는
출력 억제 규칙일 뿐 k-anonymity 보장이나 사용자 권한 부여가 아니다.

차단 또는 추가 확인:

- raw SQL, row projection, OR/NOT, 부분 문자열·자유 filter와 계산식
- timestamp timezone, relative time, fiscal/cohort 기간 기준
- 단위 변환
- composite FK, parent-to-child fan-out, 동률 join path
- PII, credential-like, 서술문, 식별자형 문자열 컬럼
- 관리자 공개 승인을 받지 않은 불확실한 문자열 차원

미지원 조건을 버린 채 결과를 내지 않는다. 조건이 남으면 `NEEDS CLARIFICATION`
또는 `BLOCKED`로 끝난다. 현재 실제 안전 실행 증거는 existing file-backed SQLite와
DuckDB에 한정된다. 다른 DB 커넥터는 연결 가능성과 timeout/취소가 검증된 질의 실행
범위를 구분하며, 검증되지 않은 원격 dialect의 governed 실행은 차단한다.

### 막혔을 때 무엇을 해야 하나

| 화면 또는 코드 | 의미 | 다음 행동 |
|---|---|---|
| `NEEDS CLARIFICATION` | 질문의 지표·분류·필터·기간을 현재 후보만으로 확정할 수 없음 | 안내된 항목을 구체적으로 다시 질문하거나 관리자가 표현을 연결 |
| `ReviewRequired` / `/semantic_reviews` | 업무 의미 또는 공개 범위를 사람이 확인해야 함 | 허용된 선택지 중 하나를 승인·거절한 뒤 원 요청자가 질문 재개 |
| 토큰 만료 또는 stale source | 15분 검토 토큰이 만료됐거나 DB가 재연결됨 | 후보 조회 또는 경고 단계부터 새 토큰 발급 |
| `BLOCKED`와 unsupported obligation | 현재 typed plan이 지원하지 않는 조건이 있음 | 조건을 제거하지 말고 지원 범위를 확장하거나 다른 분석 경로 선택 |
| contributor·disclosure 차단 | 결과가 너무 작거나 차원이 아직 공개되지 않음 | 데이터 공개 정책을 확인하고, 허용되는 경우에만 관리자 검토 |
| catalog/audit 오류 | 의미 변경이나 실행을 안전하게 기록할 수 없음 | 저장소 오류를 복구한 뒤 새 plan으로 다시 시도 |

차단 사유를 무시하고 `run_sql`로 우회하는 것은 업무 의미 검토형 질의 모드의 복구
방법이 아니다.

## 기타 기존 명령

`/ingest`, `/confirm_ingest`, `/term_custom`, `/remember`, `/audit_me`는 기존
기능으로 남아 있다. `/enrich`와 `/org_setup`의 raw-value sampling은 업무 의미
검토형 질의가 활성화된 DB에서는 의도적으로 비활성화된다.
