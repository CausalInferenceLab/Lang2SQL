# Architecture — 기여자용 한눈 가이드

이 문서는 *처음 보는 사람도 10분 안에 어디 무엇이 있는지 / 어디를 손대면 좋은지* 알 수 있도록 쓰여졌습니다. 상세 설계 의도는 [`docs/discord_first_redesign_v4_1.md`](./discord_first_redesign_v4_1.md)에 있습니다.

> **두 질의 모드를 구분해서 읽어 주세요.** semantic catalog가 없는 연결은 아래의
> legacy `run_sql` 경로를 사용한다. `/setup`으로 catalog가 활성화된 연결은 모델에서
> `run_sql`을 제거하고, [`연결 즉시 의미 준비형 질의`](./REVIEWED_SEMANTIC_QUERY.md)의
> 검토 기반 경로를 사용한다. 임베딩 애플리케이션은 모델이 SQL을 작성하지 않는
> [`Lang2SQLRuntime`](./LIBRARY_API.md)을 사용할 수 있다.

---

## 1. 한 눈에 보는 아키텍처

```
   USER (Discord / CLI / 향후 Slack·Web)
       │
       ▼
┌─────────────────────────────────────────────────┐
│  frontends/  ← 입력 받고 출력 보내기 (transport)    │
│   discord/   cli/   slack/(빈)   web/(빈)         │
└──────────────────┬──────────────────────────────┘
                   ▼   (인터랙션 → Identity)
┌─────────────────────────────────────────────────┐
│  tenancy/  ContextConcierge ← *조립점*            │
│   요청마다 HarnessContext를 하나 만들어 넘김         │
└──────────────────┬──────────────────────────────┘
                   ▼   (ctx = LLM+tools+session+...)
┌─────────────────────────────────────────────────┐
│  harness/  agent_loop                            │
│   system prompt → LLM → tool 호출 → 다음 턴/종료   │
└──────────────────┬──────────────────────────────┘
                   ▼   (도구가 ctx의 포트를 호출)
┌──────────────┬───────────┬───────────┬─────────┐
│ semantic/(★④)│safety/(★①)│memory/(★②)│ingest/(★③)│  ← 4기둥
└──────────────┴───────────┴───────────┴─────────┘
                   │
                   ▼   (모두 포트(Protocol)로 외부와 분리)
┌─────────────────────────────────────────────────┐
│  adapters/  외부 시스템과의 마지막 한 줄              │
│   llm/openai_ · llm/fake                          │
│   db/sqlalchemy_explorer · db/d1_explorer · db/postgres_explorer │
│   storage/sqlite_store · storage/sqlite_semantic   │
└─────────────────────────────────────────────────┘
```

핵심 원칙: **로직은 포트(추상)에만 의존, 어댑터(구체)는 가장자리에만**. 그래서 새 LLM·새 DB·새 frontend를 *기존 코드 안 건드리고* 끼울 수 있습니다.

### 연결 즉시 의미 준비형 질의 경로

이 경로는 기존 4기둥을 대체하거나 다섯 번째 기둥을 추가하지 않는다. 같은
frontend·tenancy·agent loop 안에서 **질의 도구와 실행 계약만** 연결별로 바꾼다.
첫 연결은 DB comment를 후보로 사용하고, source·물리 fingerprint가 같은 재연결은
기존 Enrich 설명 캐시도 후보로 재사용한다. 어느 쪽도 승인된 업무 의미·집계·공개
정책으로 자동 승격하지 않는다.

```text
Discord 또는 공개 API
  → 질문별 후보(shortlist)
  → 사람 검토가 필요한 의미·공개 범위 확인
  → SQL 없는 typed plan 검증
  → 결정론적 compiler
  → safety + 결과 공개 정책
  → read-only SQLite/DuckDB 실행
```

이 경로에서 모델은 후보 ID와 질문에 실제 등장한 표현을 구조화할 뿐 SQL을 만들거나
받지 않는다. 지원하지 않는 필터·기간·join·dialect는 legacy 경로로 되돌리지 않고
typed blocker로 끝난다.

---

## 2. 왜 이런 구조? — 4기둥 (해결하려는 문제)

| ★ | 이름 | 풀려는 현실 문제 | 핵심 파일 |
|---|---|---|---|
| ① | **Safety pipeline** | SQL이 *실수로/악의로* DB를 망치는 일 | [`src/lang2sql/safety/`](../src/lang2sql/safety/) |
| ② | **Memory 3축** | 봇이 어제 한 얘기·정의를 *기억 못 함* | [`src/lang2sql/memory/`](../src/lang2sql/memory/) |
| ③ | **Ingestion 매트릭스** | 비즈니스 정의를 *사람이 일일이* 입력해야 함 | [`src/lang2sql/ingestion/`](../src/lang2sql/ingestion/) |
| ④ | **Semantic federation** | 같은 *"활성 사용자"* 가 팀마다 의미 다름 | [`src/lang2sql/semantic/`](../src/lang2sql/semantic/) |

자세한 배경은 redesign 문서 §3을 참고.

---

## 3. 디렉토리·레이어 가이드

> 의존 방향: `frontends → tenancy → harness → semantic/safety/memory/ingestion/tools → core ← adapters`
> `core/`는 누구도 의존하지 않는 *순수* 영역(타입+포트). 새 모듈 추가 시 이 방향을 깨지 않게.

### `src/lang2sql/core/` — 순수 타입 + 포트 (★ 손대지 마세요)
시스템 전체의 *어휘*가 모여 있습니다. 외부 의존 0, I/O 0.
- [`types.py`](../src/lang2sql/core/types.py) — `Message`, `ToolCall`, `ToolResult`, `Completion`, `Role`
- [`identity.py`](../src/lang2sql/core/identity.py) — `Identity`, `Scope`, federation의 `scope_chain()` 순서 (narrow→wide)
- [`ports/`](../src/lang2sql/core/ports/) — Protocol: `LLMPort`, `ExplorerPort`, `ToolPort`, `SafetyLayerPort`, `SafetyPipelinePort`, `StorePort`, `RecallPort`, `ExtractorPort` (memory), `SourcePort`, `DocExtractorPort`, `FrontendPort`, `SecretsPort`, `SessionStorePort`, `AuditPort`

### `src/lang2sql/harness/` — 에이전트 한 턴의 엔진
- [`context.py`](../src/lang2sql/harness/context.py) — `HarnessContext` (llm + tools + safety + explorer + store + session 한 다발)
- [`session.py`](../src/lang2sql/harness/session.py) — 대화 transcript
- [`loop.py`](../src/lang2sql/harness/loop.py) — `agent_loop`: system prompt → LLM → tool 호출 → 다음 턴
- [`tool_registry.py`](../src/lang2sql/harness/tool_registry.py) — 이름→도구 dispatch
- [`system_prompt.py`](../src/lang2sql/harness/system_prompt.py) — 시멘틱 + 스키마 주입

### `src/lang2sql/semantic/` — 업무 의미, 검토, 계획, 실행 정책 (★④)
- [`catalog.py`](../src/lang2sql/semantic/catalog.py) — 연결별 물리 사실과 검토된 업무 의미
- [`onboarding.py`](../src/lang2sql/semantic/onboarding.py) — PII-safe metadata-only 초기 연결 scan
- [`shortlist.py`](../src/lang2sql/semantic/shortlist.py) — 질문별 bounded candidate 생성
- [`plan.py`](../src/lang2sql/semantic/plan.py) — SQL 없는 semantic plan IR
- [`compiler.py`](../src/lang2sql/semantic/compiler.py) — 검증된 plan의 결정론적 SQL 컴파일
- [`execution.py`](../src/lang2sql/semantic/execution.py) — read-only 실행, audit, 결과 공개 gate
- [`service.py`](../src/lang2sql/semantic/service.py) — 검토와 Discord semantic-query lifecycle
- 기존 federation 로직은 [`tools/semantic_federation.py`](../src/lang2sql/tools/semantic_federation.py)에 KV 기반으로 유지

### `src/lang2sql/safety/` — Read-only 게이트 (★①)
- [`pipeline.py`](../src/lang2sql/safety/pipeline.py) — layer를 순서대로 통과, *첫 비-PASS에서 차단*
- [`layers/whitelist.py`](../src/lang2sql/safety/layers/whitelist.py) — SELECT/WITH만 통과, DML 키워드 fail-closed
- [`layers/timeout.py`](../src/lang2sql/safety/layers/timeout.py) — 실행 timeout config
- [`tests/test_safety.py`](../tests/test_safety.py) — **12개 회귀 케이스** (머지 게이트)

### `src/lang2sql/memory/` — Hermes 3축 (★②)
- [`stores/in_memory.py`](../src/lang2sql/memory/stores/in_memory.py) — Where
- [`recall/inject_all.py`](../src/lang2sql/memory/recall/inject_all.py) — What
- [`extractors/manual.py`](../src/lang2sql/memory/extractors/manual.py) — How new
- [`service.py`](../src/lang2sql/memory/service.py) — 셋을 묶음

### `src/lang2sql/ingestion/` — 문서 → 시멘틱 후보 (★③)
- [`sources/file_source.py`](../src/lang2sql/ingestion/sources/file_source.py) — 어디서
- [`extractors/llm_extractor.py`](../src/lang2sql/ingestion/extractors/llm_extractor.py) — 어떻게 추출
- [`pipeline.py`](../src/lang2sql/ingestion/pipeline.py) — Source × Extractor matrix

### `src/lang2sql/tools/` — 에이전트가 부르는 capability
대표 도구는 모두 ctx-aware, async다. 연결 모드에 따라 질의 도구가 달라진다.
- [`run_sql.py`](../src/lang2sql/tools/run_sql.py) — catalog가 없는 legacy 연결에서만 safety 통과 후 explorer로 실행
- [`semantic_query.py`](../src/lang2sql/tools/semantic_query.py) — 연결 즉시 의미 준비형 질의 연결에서 typed slots만 받고 서버가 SQL을 컴파일
- [`explore_schema.py`](../src/lang2sql/tools/explore_schema.py) — 테이블/컬럼 introspection
- [`enrich_schema.py`](../src/lang2sql/tools/enrich_schema.py) — LLM으로 컬럼 메타데이터 자동 보강
- [`semantic_federation.py`](../src/lang2sql/tools/semantic_federation.py) — `term_custom`: guild/channel/member 계층 용어 사전 (KV 기반, narrow→wide lookup)
- [`org_setup.py`](../src/lang2sql/tools/org_setup.py) — 전사/팀 단위 용어 일괄 등록
- [`remember.py`](../src/lang2sql/tools/remember.py) — fact 저장
- [`ask_user.py`](../src/lang2sql/tools/ask_user.py) — 모호하면 사용자에게 질문
- [`ingest_doc.py`](../src/lang2sql/tools/ingest_doc.py) — 문서 → 후보 제안
- [`__init__.py: build_default_tools`](../src/lang2sql/tools/__init__.py) — 어셈블리

### `src/lang2sql/tenancy/` — 조립점
- [`concierge.py`](../src/lang2sql/tenancy/concierge.py) — *유일하게* 구체 클래스를 import 하는 곳. 요청마다 `HarnessContext` 만듦.
- [`encrypted_secrets.py`](../src/lang2sql/tenancy/encrypted_secrets.py) — `cryptography.Fernet` 실 암호화

### `src/lang2sql/adapters/` — 외부 시스템과의 마지막 줄
- `llm/openai_.py` — urllib 기반 OpenAI tool-calling
- `llm/fake.py` — 오프라인 테스트용 결정적 LLM
- `db/sqlalchemy_explorer.py` — **DSN만 바꾸면 Postgres/MySQL/Snowflake/BigQuery/DuckDB 다 커버**
- `db/d1_explorer.py` — Cloudflare D1 (HTTP API, urllib)
- `db/factory.py` — `build_explorer(connection)` scheme 라우팅
- `db/postgres_explorer.py` — V1 stub (psycopg 미설치 환경용)
- `storage/sqlite_store.py` — `AuditPort` + `SessionStorePort` + kv
- `storage/sqlite_semantic.py` — 시멘틱 정의 영속화

Connector가 DSN을 해석할 수 있다는 사실과 governed execution이 검증됐다는 사실은
다르다. 현재 compiler·bound parameter·timeout/cancel·read-only 실행까지 검증된
governed dialect는 기존 파일 기반 SQLite와 DuckDB뿐이며, 나머지는 fail-closed한다.

### `src/lang2sql/frontends/` — 사용자 인터페이스
- [`discord/bot.py`](../src/lang2sql/frontends/discord/bot.py) — **유일하게** `discord.py`를 import
- [`discord/commands.py`](../src/lang2sql/frontends/discord/commands.py) — 순수 핸들러 (discord 비의존, 테스트 가능)
- [`discord/setup_wizard.py`](../src/lang2sql/frontends/discord/setup_wizard.py) — `/setup` Modal/Select
- [`discord/session_router.py`](../src/lang2sql/frontends/discord/session_router.py) — discord ID → `Identity`
- [`discord/render.py`](../src/lang2sql/frontends/discord/render.py) — >50행이면 CSV 첨부
- [`cli/app.py`](../src/lang2sql/frontends/cli/app.py) — 개발용 CLI

---

## 4. 한 메시지의 lifecycle (디스코드 멘션 한 번 따라가기)

### 연결 즉시 의미 준비형 질의 모드: catalog가 있는 연결

```text
1. 사용자가 자연어 질문을 보낸다.
2. bot/commands가 Identity와 원 질문을 그대로 ContextConcierge에 넘긴다.
3. ContextConcierge가 연결의 semantic catalog를 확인하고 run_sql 대신 semantic_query를 등록한다.
4. 모델은 질문별 metric/dimension/filter/date 후보에서 typed slot을 조립한다.
5. 아직 확인되지 않은 의미나 공개 범위가 있으면 ReviewRequired로 멈춘다.
6. 사람이 허용된 선택지를 고르면 같은 프로세스에서는 원 draft를 재해석 없이 재개한다.
7. compiler가 allowlisted ID, aggregate, join, bound filter와 기간을 검증해 SQL을 만든다.
8. safety, contributor 보호, 공개 정책, audit와 catalog stamp가 모두 통과해야 결과를 표시한다.
```

정확한 Discord 검토 흐름은 [`REVIEWED_SEMANTIC_QUERY.md`](./REVIEWED_SEMANTIC_QUERY.md),
다른 애플리케이션의 공개 DTO 흐름은 [`LIBRARY_API.md`](./LIBRARY_API.md)를 따른다.

### Legacy mode: catalog가 없는 연결

```
1. 사용자: "@lang2sql-test 이번 달 매출 알려줘"
2. discord/bot.py: on_message → _message_context()로 (guild_id, channel_id, user_id) 뽑음
3. session_router.to_identity()  →  Identity(...)
4. CommandHandlers.query(identity, "이번 달 매출 알려줘")
5. ContextConcierge.build_context(identity)
     - secrets에서 길드별 db_dsn 있나? → 있으면 build_explorer로 그 DB 사용 (캐시)
     - SqliteStore에서 세션 로드 (없으면 새로)
     - build_default_tools()로 ToolRegistry 채움
     - HarnessContext 반환
6. agent_loop(ctx, "이번 달 매출 알려줘")
     - system_prompt: 시멘틱 effective_layer + 스키마 주입
     - LLM(GPT-4.1-mini): "run_sql 도구를 부르세요" 응답
     - tools.dispatch("run_sql", {sql: "SELECT ..."}, ctx)
        → safety.evaluate(sql) → PASS
        → explorer.execute(sql) → 행들 반환
     - 결과 messages에 추가, LLM 다시 호출 → 최종 답변
7. concierge.store.save(session_key, ctx.session)  ← 세션 영속화
8. render_answer(answer) → OutboundMessage
9. interaction.followup.send(...)  → Discord에 답
```

---

## 5. 어디를 수정하면 좋을까 — Extension Points

기여 PR을 받기 가장 쉬운 지점들. 전부 *기존 코드 안 건드리고 추가만 하면 됩니다*.

### LLM 추가 (예: Anthropic Claude, NIM)
1. `src/lang2sql/adapters/llm/<provider>_.py` 새로 작성, `LLMPort` 구현
2. `tenancy/concierge.py: _default_llm()`에 분기 추가
3. tests/ 에 `test_<provider>_adapter.py`

### 새 DB 지원
SQLAlchemy 지원 DB라면:
1. `pyproject.toml`의 `[project.optional-dependencies]`에 extra 추가
2. 끝. `SqlAlchemyExplorer`가 DSN으로 알아서 처리

SQLAlchemy 미지원 (예: 자체 HTTP API):
1. `adapters/db/<db>_explorer.py`에 `ExplorerPort` 구현
2. `adapters/db/factory.py`의 `build_explorer`에 scheme 분기
3. `adapters/db/dsn_builder.py`에 `build_<db>()` + `FIELD_SCHEMA[<db>]`
4. tests/

### 새 safety layer (예: AST 정밀 검증, 함수 차단, EXPLAIN 비용)
1. `safety/layers/<name>.py`에 `SafetyLayerPort` 구현
2. `safety/pipeline.py`의 `SafetyPipeline` 기본 layers 목록에 끼우거나, 옵셔널로 노출
3. tests/test_safety.py에 회귀 케이스 추가

### 더 똑똑한 memory recall (예: 키워드, 벡터)
1. `memory/recall/<name>.py`에 `RecallPort` 구현
2. concierge에서 옵션으로 선택 가능하게
3. tests/

### 새 ingestion source (예: URL, Notion MCP)
1. `ingestion/sources/<name>.py`에 `SourcePort` 구현
2. ingestion 도구 흐름이 자동 매트릭스이므로 추가 코드 거의 없음

### 새 frontend (예: Slack, Web)
1. `frontends/<platform>/` 디렉토리에 transport 작성
2. `commands.py`는 그대로 재사용 (discord 비의존이라)
3. `core/ports/frontend.py`의 `FrontendPort` 인터페이스 따르기

### 새 도구 (예: visualize, write_code)
1. `tools/<name>.py`에 `ToolPort` 구현 (spec + run)
2. `tools/__init__.py: build_default_tools()`에 추가
3. tests/

---

## 6. 빠른 기여 시작 (5분)

```bash
git clone https://github.com/CausalInferenceLab/Lang2SQL.git
cd Lang2SQL
uv sync                          # 기본 deps
.venv/bin/pytest -q              # 106 테스트 통과 확인
.venv/bin/python bench/ecommerce_demo.py   # federation + safety 로컬 데모
```

브랜치 → 코드 + 테스트 → PR. CI는 따로 없으니 *로컬에서 pytest 확인 후 PR*.

---

## 7. 코드 컨벤션 (작은 약속)

| 규칙 | 이유 |
|---|---|
| **포트는 `typing.Protocol`** (`runtime_checkable` 권장) | 덕타이핑 + isinstance 가능 |
| **어댑터의 engine/connection은 lazy** | 라우팅 단계에서 드라이버 미설치여도 OK |
| **blocking 호출은 `asyncio.to_thread`** | discord 이벤트 루프 막지 않기 |
| **frontends/discord에서 `discord.py` import는 `bot.py`·`setup_wizard.py`만** | 로직층은 유닛테스트 가능해야 함 |
| **새 환경변수는 `.env.example`에도 문서화** | 신규 컨트리뷰터 친화 |
| **테스트는 토큰/네트워크 없이도 통과해야 함** | `FakeLLM` / mock transport 활용 |
| **포트(`core/ports/`)는 거의 frozen** | 변경은 모든 어댑터/구현에 영향 — 정말 필요한지 한 번 더 고민 |

---

## 8. 더 깊이 보고 싶다면

- [`docs/discord_first_redesign_v4_1.md`](./discord_first_redesign_v4_1.md) — *왜 이렇게 만들었나* (장문)
- [`docs/discord_first_redesign_v4_2.md`](./discord_first_redesign_v4_2.md) — 확정 컨셉 요약 (단문)
- [`docs/DEPLOY.md`](./DEPLOY.md) — Discord 봇 운영
- [`bench/ecommerce_demo.py`](../bench/ecommerce_demo.py) — federation/safety 라이브 데모
- 테스트가 사실상 사양서 — `tests/test_*.py`를 *모듈별 가이드*로 활용

---

질문/제안은 [Discord](https://discord.gg/EPurkHVtp2) 또는 GitHub Issues 환영.
