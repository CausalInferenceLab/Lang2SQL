# Lang2SQL Discord 사용 가이드

이 실험 브랜치는 LLM이 SQL을 직접 쓰지 않는다. LLM은 질문을 검토 가능한
값으로 조립하고, 서버가 확인된 값만으로 read-only SQL을 만든다.

## 준비

실제 질문에는 tool calling이 가능한 모델이 필요하다. `OPENAI_API_KEY`를
설정하거나 Ollama 같은 OpenAI-compatible 서버를 연결한다.

```bash
export LANG2SQL_LLM_BASE_URL=http://127.0.0.1:11434
export LANG2SQL_LLM_MODEL=gemma4:26b
export DISCORD_BOT_TOKEN=...
.venv/bin/lang2sql-bot
```

모델 설정이 없으면 `FakeLLM`이 사용된다. 이는 설치 확인용일 뿐 의미 있는
자연어 질의 검증용이 아니다.

## 1. `/setup`으로 DB 연결

Discord 서버 관리자가 `/setup`을 실행하고 DB 종류와 접속 정보를 입력한다.
지원 선택지는 SQLite, PostgreSQL, MySQL, Snowflake, BigQuery, DuckDB, D1이다.
credential-bearing DSN을 채널 명령으로 직접 받는 `/connect`는 노출하지 않는다.

처음 로컬 검증은 SQLite가 가장 단순하다.

1. `/setup`
2. `SQLite` 선택
3. 봇 프로세스에서 접근 가능한 DB 파일의 절대 경로 입력

연결 성공 메시지는 테이블 수, 선언 FK 수, 민감/자유 텍스트 차단 수를 보여준다.
연결 단계에서는 업무 지표를 전부 묻지 않는다.

## 2. 봇을 멘션해 질문

```text
@Lang2SQL Amount by region name
```

모든 채널, thread, DM에서 명시적 `@Lang2SQL` 멘션이 필요하다. 봇은 일반 대화,
`@everyone`, `@here`를 질의로 가로채지 않는다.

처음 보는 표현이면 봇이 다음처럼 실제 연결을 보여준다.

```text
amount → orders.amount
선택: SUM / AVG / MIN / MAX / COUNT / reject
```

`/semantic_review`에서 선택하면 승인 당시의 metric, aggregate, dimensions,
질문을 그대로 사용해 실행한다. LLM이 원 질문을 다시 해석하지 않는다. 같은
표현·집계 조합은 이후 재확인하지 않는다.

## 3. 상태와 복구

- `/semantic_status`: 자동 구조와 확인된 표현·집계 연결 수 확인
- `/semantic_review`: 현재 질문의 연결 확인 또는 거절
- `/semantic_reset confirm:true`: 사람이 확인한 연결 전체 초기화(관리자)

초기 실험에서는 전용 guild/channel과 신뢰된 reviewer 한 명을 권장한다. 현재
확인 결과는 guild catalog에 공유되며, 역할별 semantic 승인 정책은 후속 범위다.

## 현재 질의 범위

지원:

- numeric metric의 SUM/AVG/MIN/MAX
- PK 기반 source-record COUNT
- categorical group-by
- 선언 FK의 유일한 child-to-parent join

차단 또는 추가 확인:

- 자유로운 filter와 계산식
- 기간/cohort 기준
- 단위 변환
- composite FK, fan-out, 동률 join path
- PII, credential-like 컬럼, 검토되지 않은 free-text

미지원 조건을 버린 채 결과를 내지 않는다. 조건이 남으면 `NEEDS
CLARIFICATION` 또는 `BLOCKED`로 끝난다.

## 기타 기존 명령

`/ingest`, `/confirm_ingest`, `/term_custom`, `/remember`, `/audit_me`는 기존
기능으로 남아 있다. `/enrich`와 `/org_setup`의 raw-value sampling은 semantic
first-connect가 활성화된 DB에서는 의도적으로 비활성화된다.
