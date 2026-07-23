# Lang2SQL v4.1 — 컨셉과 아키텍처

> **역사적 설계 문서**: 아래 명령 표는 2026-05 계획 당시의 walking skeleton이다.
> 현재 실행 표면은 [`USAGE.md`](./USAGE.md)와
> [`REVIEWED_SEMANTIC_QUERY.md`](./REVIEWED_SEMANTIC_QUERY.md)를 따른다. 현재
> Discord에는 raw DSN `/connect`가 없고 `/setup`을 사용한다.

> **작성일**: 2026-05-18
> **결정자**: ryan@brain-crew.com
> **상태**: v4 → v4.1. 두 가지 큰 정정 — (a) Discord 를 *기둥* 에서 *Phase 1 인터페이스* 로 격하, (b) **시멘틱 federation (git-like 분기)** 을 4번째 ★ 패턴으로 추가.
> **전제**: 기존 레포 wipe 후 백지에서 다시 작성.

---

# Chap 1. 만들려는 것의 정체성

## 1.1 기존 오픈소스의 공통 한계

Vanna AI(~20k), Wren AI(~12k), SQLCoder(~3.8k) 같은 Text-to-SQL 오픈소스들은 *질문→SQL 파이프라인 자체* 는 이미 완성도가 높음. 우리는 *그 위에 무엇을 더 얹을* 것인가가 정체성 결정.

기존 오픈소스의 **공통 약점**:

| 약점 | 누구의 문제 | 기존 처리 |
|---|---|---|
| **DB 정보 완성도 낮으면 성능 급락** | 사용자가 description/sample 없이 등록 | Vanna: 학습 데이터 품질에 전적 의존 |
| **이전 대화·정의 못 기억** | 매번 새 세션처럼 동작 | 대부분 stateless |
| **비즈니스 맥락은 사람이 직접 정의** | metric·용어집을 일일이 입력 | Wren: MDL 수동 작성 |
| **단일 진실 (single truth) 모델의 조직 충돌** | 같은 회사라도 팀별로 *"활성 사용자"* 정의가 다름. 한 팀이 바꾸면 다른 팀 깨짐 | Wren: 단일 MDL. Vanna: 학습 데이터 혼선 |

이 네 가지가 *실무에서 매우 중요* 한데 *비즈니스마다 다르기 때문에 정량 평가가 어려운 영역*. 그래서 오픈소스가 잘 안 건드림. 우리는 **이 네 가지를 한 번에 다루는 방향** 으로 차별화.

## 1.2 우리의 한 줄 컨셉

> **"문서로 비즈니스 맥락을 학습하고, 팀별로 시멘틱이 분기되고, 불완전한 DB에서도 답하고, 모든 정의·대화를 기억하는 오픈소스 분석 에이전트."**

기존 SQL 봇이 *"질문하면 SQL 만들어 줄게"* 였다면, 우리는 *"우리 회사 문서 넣어줘 → 학습할게 → 팀별로 다른 정의도 같이 들고 있을게 → 너희가 묻고 답을 받자 → 다 기억하고 다음에도 적용할게"*.

**Discord 는 Phase 1 인터페이스**. Slack/Web/Teams 도 같은 코어 위에서 어댑터로 추가 가능. *Discord 자체* 는 정체성이 아니라 *멀티 인터페이스* 의 첫 구현.

## 1.3 4기둥 (Pillars)

| 기둥 | 정체성 | 차별점 |
|---|---|---|
| **① 비즈니스 맥락 학습** | 문서→시멘틱 자동 추출 + 사용자 confirm | Wren MDL 은 수동 작성. 우리는 *문서가 곧 진실의 출처* |
| **② 강건성 (2축)** | (2a) **DB 강건성** — 불완전 메타데이터 자동 보강 (2b) **시멘틱 강건성** — 팀별 다른 정의 federation | 기존 오픈소스 *없는 영역*. 우리의 한 포인트 |
| **③ Hermes 기억** | conversation + facts + preferences 영속 | 대부분 stateless |
| **④ 멀티 인터페이스** | Phase 1: Discord, Phase 2: Slack, Phase 3: Web | Discord는 *어댑터 하나*, lock-in 없음 |

기둥 ②가 *우리 제품·연구 정체성*. **두 종류의 강건성** 이 핵심:
- **(2a) DB 강건성** — 사용자가 description/sample query 없이 등록해도 성능 하락 최소화
- **(2b) 시멘틱 강건성** — 팀별 정의 차이를 *충돌 없이 공존* (git-like 브랜칭)

V1엔 골격만, V1.5/V2에서 깊이 채움.

## 1.4 타이브레이커 (충돌 시 우선순위)

1. **무결성 + 권한** — DB가 변하지 않고, 권한 있는 데이터만 본다
2. **의미적 정확성** — 답이 *진짜로* 맞는다
3. **UX 단순성** — 한 번에 결과 (멘션→thread, 또는 다른 frontend 의 동치 패턴)
4. **맥락 보존** — 끊겨도 이어진다 (Hermes 영속화)

---

# Chap 2. 아키텍처 — ASCII로 보는 큰 그림

## 2.1 전체 흐름

```
   USER  (Phase 1: Discord. Phase 2+: Slack/Web/Teams)
       │
       ▼
┌─────────────────────────────────────────────────────────┐
│  ★ Frontend 어댑터  (인터페이스 분리)                       │
│                                                           │
│   discord/  ──┐                                           │
│   slack/   ──┤   각 어댑터가 공통 인터페이스 구현            │
│   web/     ──┘   - 입력 받기                               │
│                  - 출력 보내기                              │
│                  - 세션 키 결정                             │
└──────────────────┬──────────────────────────────────────┘
                   ▼
┌─────────────────────────────────────────────────────────┐
│  MEMORY + CONCIERGE                                      │
│                                                           │
│   기억 (Memory) — 3축 분리                                 │
│     • 어디 저장하는가                                       │
│     • 무엇을 가져올지                                       │
│     • 어떻게 새로 만드는지                                  │
│                                                           │
│   문서 흡수 (Ingestion) — 출처 × 해석                       │
│     • 어디서 문서를 받는가 (파일/URL/Notion ...)             │
│     • 어떻게 metric/rule 을 뽑아낼지                         │
│                                                           │
│   시멘틱 federation — scope 결정                            │
│     • 이 대화가 어느 scope 인가 (DM/채널/스레드)             │
│     • 같은 용어 다른 정의 시 어느 게 우선인가                 │
│                                                           │
│   ContextConcierge — 위 셋을 묶어 ctx 조립                   │
└──────────────────┬──────────────────────────────────────┘
                   ▼ ctx
┌─────────────────────────────────────────────────────────┐
│  ★ HARNESS  (조립된 단위, 하나의 덩어리)                    │
│                                                           │
│   ctx 안에 있는 것:                                        │
│     • LLM 클라이언트                                       │
│     • 도구들 (run_sql, ingest_doc, ...)                    │
│     • semantic (scope-aware)                              │
│     • session (영속 conversation + facts)                  │
│     • safety pipeline                                      │
│     • DB explorer                                          │
│     • audit logger                                         │
│                                                           │
│   agent_loop:                                              │
│     1. system prompt 만들기                                 │
│        - 현재 scope 의 effective semantic 주입               │
│        - 관련 facts 주입                                    │
│        - DB schema 주입                                     │
│     2. LLM 호출                                            │
│     3. 도구 호출 결과 모으기                                 │
│     4. 다음 턴 또는 종료                                    │
│                                                           │
│   run_sql 도구 내부:                                        │
│     ★ safety pipeline 통과 후 실행                          │
│        layer 1 → 2 → 3 → ...                              │
└──────────────────┬──────────────────────────────────────┘
                   ▼
┌─────────────────────────────────────────────────────────┐
│  OUTBOUND ADAPTERS                                       │
│   LLM · DB (asyncpg) · Storage (encrypted SQLite)         │
└─────────────────────────────────────────────────────────┘
```

★ 4개가 **확장 핵심** — Chap 3에서 자세히.

## 2.2 디렉토리 구조

```
lang2sql/
├── README.md
├── pyproject.toml
│
├── src/lang2sql/
│   ├── core/                      # 순수 타입 + 포트(추상)
│   │   ├── types.py
│   │   ├── identity.py
│   │   └── ports/
│   │       ├── llm.py
│   │       ├── explorer.py
│   │       ├── tool.py
│   │       ├── secrets.py
│   │       ├── audit.py
│   │       ├── session_store.py
│   │       ├── frontend.py        ← NEW (Phase 분리)
│   │       ├── safety.py          ← Safety layer
│   │       ├── memory.py          ← Memory 3축
│   │       ├── ingestion.py       ← Document source × extractor
│   │       └── semantic_scope.py  ← NEW (Federation)
│   │
│   ├── harness/                   # 조립된 단위
│   │   ├── context.py
│   │   ├── session.py
│   │   ├── loop.py
│   │   ├── system_prompt.py
│   │   └── tool_registry.py
│   │
│   ├── semantic/                  # 도메인 모델 + scope 지원
│   │   ├── types.py               # Metric/Dimension/Relationship/Rule
│   │   ├── layer.py
│   │   ├── scoped_layer.py        ← NEW (git-like 분기)
│   │   ├── sql_composer.py
│   │   └── store.py
│   │
│   ├── safety/                    # ★ Safety pipeline (체인)
│   │   ├── pipeline.py
│   │   └── layers/
│   │
│   ├── memory/                    # ★ 3축 분리
│   │   ├── service.py
│   │   ├── stores/
│   │   ├── recall/
│   │   └── extractors/
│   │
│   ├── ingestion/                 # ★ 출처 × 해석 매트릭스
│   │   ├── pipeline.py
│   │   ├── sources/
│   │   └── extractors/
│   │
│   ├── tools/                     # ctx-aware
│   │   ├── run_sql.py
│   │   ├── explore_schema.py
│   │   ├── define_metric.py       # scope 인지
│   │   ├── ingest_doc.py
│   │   ├── remember.py
│   │   └── ask_user.py
│   │
│   ├── tenancy/
│   │   ├── concierge.py
│   │   └── encrypted_secrets.py
│   │
│   ├── frontends/                 # ★ 인터페이스 분리
│   │   ├── discord/               # Phase 1
│   │   ├── slack/                 # Phase 2 (디렉토리만 비워둠)
│   │   ├── web/                   # Phase 3
│   │   └── cli/                   # 개발 도구
│   │
│   └── adapters/                  # outbound
│       ├── llm/openai_.py
│       ├── db/postgres_explorer.py
│       └── storage/encrypted_sqlite.py
│
├── tests/
└── docs/
```

---

# Chap 3. 확장성 — 4개의 패턴

## 3.1 왜 패턴인가

V1엔 *가장 단순한 1개씩만* 구현, V1.5/V2에서 *어댑터를 추가* 하는 방식으로 확장. 핵심 가치: **V1엔 단순 구현으로 동작하지만 추상은 박혀 있어서 새 구현을 끼울 때 기존 코드 안 건드림**.

비유로 풀면 — *콘센트와 가전제품*. V1엔 LED 전구 하나만 꽂혀 있어도, 콘센트 규격이 표준이라 V1.5에 선풍기·스마트조명 그냥 꽂으면 됨. 콘센트 자체를 다시 만들 일 없음.

| ★ | 이름 | 역할 |
|---|---|---|
| ① | Safety pipeline | DB 강건성 — *검사 layer 줄* 에 SQL 을 통과시킴, layer 추가식 확장 |
| ② | Memory service | Hermes 기억 — *어디 저장/무엇을 가져올지/어떻게 만들지* 3축 독립 진화 |
| ③ | Ingestion pipeline | 문서 흡수 — *출처 × 해석* 매트릭스로 입력 다양화 |
| ④ | **Semantic federation** | **시멘틱 강건성 — git-like 팀별 분기** |

## 3.2 ★ ① Safety pipeline — DB 강건성의 모이는 곳

### 개념

SQL이 실행되기 전에 *검사 layer 들의 줄* 을 차례로 통과. 각 layer 는 통과/차단/사용자 확인 요청/SQL 수정 중 하나를 결정.

비유 — *공항 보안 검색대*. 가방을 X-ray 통과 → 금속탐지 → 액체 검사 → 라벨 검사. 한 단계라도 막히면 통과 불가. 새 검사항목 (예: 화학물질 탐지) 추가는 *기존 단계 그대로 둔 채 검색대 줄에 한 칸 끼우기*.

### 단계별 진화

| 버전 | 줄에 있는 layer |
|---|---|
| **V1** (껍데기) | (1) Whitelist — SELECT/WITH 로 시작해야만 통과 (2) Timeout 설정 — 30초 |
| **V1.5** | + AST 정밀 검증 (CTE 안 INSERT 도 잡음) + 위험 함수 차단 (pg_sleep 등) + LIMIT 자동 부착 + 5개 PG 설정 일괄 적용 + Rate limit + **메타데이터 자동 보강** (description 없는 컬럼 자동 생성) |
| **V2** | + 비용 게이트 (EXPLAIN 으로 예상 비용 평가, 임계 초과 시 사용자 확인) + 엔진별 별도 pipeline (PG vs BigQuery) |

V1.5 의 **메타데이터 자동 보강** layer 가 *DB 강건성 차별점의 핵심*. 사용자 DB에 description 빈 칸이면 LLM이 채워줌. 채워진 description 은 시멘틱 레이어로 흘러가 다음 질문에 활용. *Vanna 가 학습 데이터 품질에 의존* 하는 약점을 *자동 보강* 으로 메움.

새 layer 추가는 *클래스 한 개 + 줄에 끼우기*. `run_sql` 도구 코드 변경 0.

## 3.3 ★ ② Memory service — Hermes 기억의 3축 분리

### 개념

기억은 *하나의 큰 상자* 가 아니라 **세 가지 독립 기능의 조합**:

| 축 | 역할 |
|---|---|
| **Store (저장소)** | facts 를 어디에 보관하는가 (메모리/SQLite/PostgreSQL/Redis ...) |
| **Recall (불러오기 전략)** | 현재 질문에 어떤 facts 를 가져올지 (전부/키워드 매칭/벡터 유사도/하이브리드) |
| **Extractor (새 fact 만들기)** | 새 facts 를 어떻게 생성할지 (사용자 명시 명령만 / LLM이 대화에서 자동 추출) |

각 축이 독립적으로 진화. 예: Store 만 더 큰 DB로 교체 가능. Recall 만 더 똑똑하게 교체 가능.

### 단계별 진화

| 버전 | Store | Recall | Extractor |
|---|---|---|---|
| **V1** | 메모리 안 dict | 모든 facts 매번 주입 | `/remember` 명령만 |
| **V1.5** | SQLite 영속 | 키워드 매칭으로 관련 facts 만 | LLM이 대화에서 반복 패턴 추출 |
| **V2** | (같음) | 벡터 유사도 (embedding) | + 충돌 해결 (사용자가 X 했다가 not-X 시) |
| **V2.5** | PostgreSQL (멀티 인스턴스) | 하이브리드 (키워드 + 벡터 + 최근성) | + 신뢰도 점수 |

V1엔 가장 단순한 조합 (메모리 + 전부 주입 + 수동) 으로 시작하되 *3축 추상* 은 박혀 있음. V1.5에 Store 만 SQLite 로 교체하는 게 *어댑터 1개 추가* 로 끝.

## 3.4 ★ ③ Ingestion pipeline — 문서 → 시멘틱 레이어

### 개념

문서가 들어오면 두 단계:

| 단계 | 역할 |
|---|---|
| **Source (출처)** | 어디서 문서를 가져오는가 (파일 업로드/URL/Notion/Confluence/Google Drive) |
| **Extractor (해석)** | 문서 텍스트에서 metric/dimension/rule 후보를 어떻게 뽑아내는가 (LLM/DDL 파싱/하이브리드) |

Source 와 Extractor 가 *매트릭스* 라서 *Source 1개 추가 = N 개 Extractor 와 자동 결합*.

### 사용자 흐름 (V1)

1. 사용자가 Discord 에 `/ingest` 와 함께 매출 정의 문서를 첨부
2. 봇이 파일 내용을 가져와서 LLM 에 *"이 문서에서 metric/dimension/rule 을 찾아줘"* 라고 요청
3. LLM 이 후보들을 추출 (예: "total_revenue = SUM(orders.amount) WHERE status != 'cancelled'")
4. 봇이 Discord embed 로 사용자에게 보여줌:
   - 📊 METRIC: total_revenue → SUM(orders.amount)
   - 📋 RULE: exclude_cancelled (total_revenue 에 적용) → status != 'cancelled'
   - [✅ 모두 등록] [개별 선택] [❌ 취소]
5. 사용자 ✅
6. 시멘틱 레이어에 등록 (출처 문서 ID 같이 보존)
7. 이후 *"이번 달 매출"* 질문 시 자동으로 위 정의가 적용됨

### 단계별 진화

| 버전 | Sources | Extractors |
|---|---|---|
| **V1** | 파일 업로드 (MD/PDF/TXT) | LLM 추출 |
| **V1.5** | + URL 가져오기 | + DDL 파일 직접 파싱 (schema 파일에서 자동 추출) |
| **V2** | + Notion / Confluence MCP | + 하이브리드 (LLM + 규칙) |
| **V2.5** | + GitHub Markdown / Google Drive MCP | + 청크 기반 RAG (긴 문서) |

V1.5엔 같은 문서를 다시 업로드하면 *변경된 부분 diff* 표시 → 사용자가 update/keep/remove 선택. 즉 **문서가 곧 단일 진실 소스** 패턴.

## 3.5 ★ ④ Semantic federation — Git-like 시멘틱 분기

### 왜 필요한가

같은 회사라도 팀별로 *같은 용어, 다른 의미* 가 흔함:

| 용어 | 마케팅 | 프로덕트 | 파이낸스 |
|---|---|---|---|
| 활성 사용자 | 30일 내 로그인 | 7일 내 핵심 액션 | 유료 구독자 |
| 매출 | 광고 매출 | (관심 없음) | net (환불 차감) |
| 유저 | 가입자 | 활성자 | 결제자 |

기존 오픈소스는 *단일 진실 (single truth)* 모델:
- Wren MDL: 회사 단일 정의, 수동 유지 → 한 팀이 바꾸면 다른 팀 깨짐
- Vanna: 학습 데이터 혼선 → 의미 충돌이 RAG 결과에 섞임

결국 *팀별로 자체 봇 운영* 으로 갈라지거나, *모든 팀이 같은 정의에 합의* 해야 함 (실현 불가).

### 우리의 해결 — git 처럼 브랜치를 가져감

```
                          builtin (시스템 기본 = 비어 있음)
                              │
                              ▼
                       ┌──────────────┐
                       │  main 브랜치  │ ← 회사 공통 시멘틱 (canonical)
                       │  (guild)      │   admin 이 등록
                       └──────┬───────┘
                              │ 상속
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
       ┌──────────┐    ┌──────────┐    ┌──────────┐
       │#marketing│    │ #product │    │ #finance │
       │ 채널 브랜치│   │ 채널 브랜치│   │ 채널 브랜치│
       │          │    │          │    │          │
       │active_   │    │active_   │    │revenue=  │
       │user=30d  │    │user=7d   │    │net       │
       └──────────┘    └──────────┘    └──────────┘
              │
              │ 상속
              ▼
       ┌──────────────────┐
       │ thread 브랜치     │ ← 일시적 정의 (분석 1건 동안만)
       │                   │
       │experiment_       │
       │arm=treatment_b   │
       └──────────────────┘
```

### 어떻게 동작하나 — 가장 구체적인 scope 가 이김

사용자가 `#marketing` 채널에서 *"활성 사용자 수"* 라 물으면 봇은 시멘틱을 찾기 위해 *아래에서 위로* 순회:
1. 현재 thread 안에 정의가 있는가? → 없음
2. `#marketing` 채널에 정의가 있는가? → 있음 (*"30일 내 로그인"*) → **여기서 멈춤**

같은 사용자가 `#product` 채널에서 같은 질문:
1. thread → 없음
2. `#product` 채널 → 있음 (*"7일 내 핵심 액션"*) → 여기서 멈춤

두 채널 모두 자기 정의를 사용. **충돌 없음** — 각자 자기 scope 에서 산다.

### 사용자 흐름

`#marketing` 채널에서 새 정의 등록:
- 사용자: `/define_metric active_user "30일 내 로그인"`
- 봇: ✅ 채널 scope 에 등록 (기본 동작)
- 효과: `#marketing` 에서만 적용. 다른 채널은 그대로.

`#finance` 채널에서 다른 정의 등록:
- 사용자: `/define_metric revenue "net (환불 제외)"`
- 봇: ✅ 채널 scope

admin 이 회사 공통 등록:
- admin: `/define_metric --guild revenue "gross + tax included"`
- 봇: ✅ main 브랜치 등록
- 효과: 다른 채널엔 영향, finance 채널엔 자체 override 가 우선

현재 scope 의 effective 시멘틱 조회:
- 사용자: `/semantic show`
- 봇: *"이 채널에서 사용 중인 정의들:*
  - *active_user (채널 override): 30일 내 로그인*
  - *revenue (회사 main): gross + tax*
  - *..."*

이 채널이 main 과 다른 점:
- 사용자: `/semantic diff main` (V1.5 기능)
- 봇: *"이 채널이 main 과 다른 정의 1개: active_user 만 override"*

채널 정의를 main 으로 승격 제안:
- 사용자: `/semantic promote active_user` (V1.5 기능)
- 봇: *"admin 승인 요청 전송됨"*

### 단계별 진화

| 버전 | 기능 |
|---|---|
| **V1** | 3-scope (guild / channel / thread) 자동 resolution. `/define_metric` 은 현재 scope 기본. `/semantic show` 로 effective layer 조회 |
| **V1.5** | `/semantic diff main`, `/semantic promote` (admin 승인 flow), 충돌 알림 (admin 이 main 바꾸면 override 채널에 통지) |
| **V2** | 외부 git 저장소 동기화 (semantic-as-code: .yaml 로 export/import), cross-guild template 공유 |
| **V2.5** | branch fork & merge UI, 충돌 해결 flow, scope 별 audit (누가 언제 정의 바꿨나) |

### 의미 — *시멘틱 강건성* 이라는 새 축

이건 **DB 강건성과는 다른 축의 강건성**:

| 축 | 무엇에 견디는가 | 어떻게 |
|---|---|---|
| **(2a) DB 강건성** | *불완전한 메타데이터* | Safety pipeline 의 메타데이터 보강 layer + auto description |
| **(2b) 시멘틱 강건성** | *조직별 다른 비즈니스 정의* | Git-like scope chain (federation) |

둘 다 *"현실의 messy함에 강건"* 이라는 큰 우산. **둘 다 Vanna/Wren 이 못 하는 영역**:

| | Vanna | Wren | **Lang2SQL** |
|---|---|---|---|
| DB 메타데이터 부족 시 동작 | ❌ 학습 데이터 의존 | ❌ MDL 사전 정의 필수 | ✅ 자동 보강 |
| 팀별 다른 정의 공존 | ❌ RAG 혼선 | ❌ 단일 MDL | ✅ Scope chain |

## 3.6 V1 에 박을 추상 (정리)

V1엔 추상만 박고 단순 구현. 코어 변경 없이 V1.5에 어댑터 추가:

- 추상 (port/interface) 들이 들어갈 위치: `core/ports/safety.py`, `memory.py`, `ingestion.py`, `semantic_scope.py`, `frontend.py`
- 각 추상의 V1 단순 구현 1~2개씩만 (Safety: layer 2개, Memory: 단순 3축 조합, Ingestion: 파일+LLM, Federation: SQLite 기반 3-scope)

추상 비용: 약 +400 LOC.
V1.5/V2 절감: 약 1,500 LOC.
*초기 투자가 빠르게 회수*.

---

# Chap 4. V1 walking skeleton

## 4.1 V1 에 들어가는 것

| 영역 | V1 |
|---|---|
| Frontend | Phase 1: Discord (DM + 채널 @bot 멘션→thread + thread reply) |
| 명령 | `/connect`, `/ingest <파일>`, `/define_metric <name> "<def>"`, `/remember "..."`, `/semantic show`, `/audit me` |
| LLM | OpenAI `gpt-4.1-mini` 단일 |
| DB | PostgreSQL only |
| 응답 | 텍스트 (≤50행) 또는 CSV 첨부 (>50행) |
| Safety | Safety pipeline + V1 layer 2개 (Whitelist + Timeout) |
| Memory | Memory service + V1 (메모리 + 전부 주입 + 수동) |
| Ingestion | 파일 업로드 + LLM 추출 |
| **Semantic federation** | **3-scope resolution (guild/channel/thread). `/define_metric` 현재 scope 기본. `/semantic show` 로 effective layer 조회.** |
| 영속화 | secrets / audit / sessions / facts / semantic_entries → SQLite |
| 도구 | run_sql · explore_schema · ingest_doc · define_metric (scope 인지) · remember · ask_user |
| 호스팅 | Oracle Cloud Always Free 또는 fly.io free |

## 4.2 V1 에 *안* 들어가는 것

| 항목 | 미루는 곳 | 이유 |
|---|---|---|
| `run_code`/`write_code` (Python 실행) | **영구 삭제** | read-only 약속을 무력화 |
| Discord-native 강조 패턴 (safety embed / metadata ask / reactions / #audit channel) | V1.5 | 사용자 워크플로우 다듬는 영역, 트라이얼 피드백 후 결정 |
| 비용 게이트 (EXPLAIN) | V1.5 | L1 + L0 timeout 1차 충분 |
| 위험 함수 차단 (pg_sleep 등) | V1.5 | Whitelist 가 1차 방어 |
| 메타데이터 자동 보강 (auto description) | V1.5 | DB 강건성 핵심 layer — V1엔 추상만, 구현은 V1.5 |
| Rate limit | V1.5 | 트라이얼 두 자릿수 |
| 자동 fact 추출 | V1.5 | 수동 `/remember` 로 시작 |
| `/semantic diff` / `/semantic promote` | V1.5 | 3-scope resolution 동작 검증 후 추가 |
| URL/Notion 문서 입력 | V1.5 / V2 | 파일 업로드 하나로 시작 |
| 벡터 유사도 recall | V2 | facts 적을 땐 효과 미미 |
| Persistent View / streaming / PNG 분할 | V1.7 | Discord SDK 깊은 영역 |
| Anthropic / NIM | V1.5 (검증 후) | OpenAI 하나로 시작 |
| Slack / Web 어댑터 | Phase 2/3 | 추상은 V1에 박힘 |
| Audit hash chain | V2 | append-only SQLite 1차 충분 |
| `visualize` (PNG 차트) | V1.7 | CSV 첨부로 충분 |
| Scope 별 audit (누가 언제 정의 바꿨나) | V2 | semantic_entries 에 created_by/created_at 만 V1 |

## 4.3 V1 LOC 추정

| 영역 | LOC |
|---|---|
| core | ~650 (ports + types + identity + semantic_scope/frontend port) |
| harness | ~700 |
| semantic | ~700 (types, layer, scoped_layer, sql_composer, store) |
| safety | ~250 (pipeline + 2 layers) |
| memory | ~350 (service + 3 simple impls) |
| ingestion | ~300 |
| tools | ~750 (6 tools — define_metric 가 scope 인지) |
| tenancy | ~400 |
| frontends/discord | ~800 (adapter, bot, commands 6개, session_router, render) |
| adapters | ~500 |
| tests | ~950 |
| **총 V1** | **~6,350 LOC 신규** |

## 4.4 일정 — 4.5~5주 솔로

| 주차 | 작업 |
|---|---|
| **Week 1** | core 포트 정의 + HarnessContext, Session, agent_loop. cli 어댑터로 단위 검증 |
| **Week 2** | semantic (scoped layer 포함) + safety pipeline + 첫 도구들 (run_sql, explore_schema, define_metric) + OpenAI/PG/SQLite 어댑터. safety 회귀 12개 |
| **Week 3** | memory + ingestion + 나머지 도구 (ingest_doc, remember, ask_user) + tenancy (concierge, secrets, factstore, scope_resolver) |
| **Week 4** | Discord 어댑터 (bot, 명령 6개, session_router, render). 동의 button view 등 |
| **Week 5** | polish, e2e, README, DEPLOY 가이드, bench/ 데모 1개, CI YAML, 첫 길드 배포 |

## 4.5 회귀 12개 (V1 CI gate)

V1 의 Whitelist + Timeout 만으로 충분히 막혀야 하는 12개 회귀 케이스:

| # | 입력 | 기대 |
|---|---|---|
| 1 | `DROP TABLE users` | 차단 |
| 2 | `; DELETE FROM t; --` | 차단 (multi-stmt) |
| 3 | `INSERT INTO t VALUES (1)` | 차단 |
| 4 | `UPDATE t SET x=1` | 차단 |
| 5 | `WITH x AS (INSERT INTO t ...) SELECT * FROM x` | 차단 (INSERT 키워드 fail-closed) |
| 6 | `SELECT * FROM nonexistent` | PG error (gate 통과, 실행 단계 실패) |
| 7 | `SELECT pg_sleep(60)` | 30초 timeout |
| 8 | `SELECT * FROM huge_table` (50k rows) | row_limit 1000 으로 truncate |
| 9 | `SELECT 1` | 통과 |
| 10 | `WITH a AS (SELECT 1) SELECT * FROM a` | 통과 |
| 11 | `EXPLAIN SELECT 1` | 통과 |
| 12 | 빈 문자열 | 차단 (parse_error) |

V1.5 에 AST 정밀 검증 + 함수 차단 + 메타데이터 보강이 들어오면 추가 회귀:
- schema-qualified bypass (`public.pg_sleep`)
- `COPY ... TO PROGRAM`
- `EXPLAIN ANALYZE DELETE`

---

# Chap 5. 의견 정리

## 5.1 왜 이 설계가 작동할 거라 보는가

**(1) "오픈소스의 정체성" 이 차별점에 부합**

Vanna/Wren/SQLCoder 가 *질문→SQL* 은 잘 풀고 있음. *"GPT-4 보다 더 좋은 SQL을 만든다"* 로 경쟁하면 모델 fine-tuning 싸움 — 우리 영역 아님.

대신 우리는 **네 가지 조합** 으로 차별:
- 문서로 비즈니스 맥락 학습
- 팀별 시멘틱 federation (git-like)
- 불완전 DB에서도 동작
- 모든 정의·대화 기억

이 조합은 기존 오픈소스에 *없음*.

**(2) 강건성을 *두 축* 으로 분리한 게 깊이**

기존엔 *"DB 강건성"* 만 봤는데, 라이브 사용해 보면 *"팀별 정의 충돌"* 이 더 자주 터지는 문제. *시멘틱 강건성* 을 별도 축으로 명시한 것 자체가 새로움.

**(3) 4개 패턴이 *연구→제품* 토대**

DB 강건성 차별화는 *"description 없으면 자동 생성"* 같은 *상황별 커스텀 전략* 의 모음. Safety pipeline 의 layer 로 들어옴.
시멘틱 강건성은 *"팀별 다른 정의"* 의 federated 관리. Scoped semantic layer 로 들어옴.
연구자가 *Hint Vector* 같은 *DB 구조 강건성* 을 추가하고 싶으면 layer 1개로 됨. *연구→제품* 흐름 자연스러움.

**(4) Discord 는 *Phase 1 인터페이스* 일 뿐**

정체성에 Discord 가 박혀 있으면 위험. v4.1 은 frontend 추상으로 분리. Phase 2 Slack 추가 시 코어 변경 0줄. *오픈소스* 정체성과도 부합 — *플랫폼 lock-in 없음*.

## 5.2 한계와 trade-off (정직)

| 한계 | 영향 | 대응 |
|---|---|---|
| 의미적 정확성 정량 평가 부재 | *"안전하게 틀린 답"* 위험 | V1.5 에 골든 query set + 답 출처 표시 |
| LLM 비용 노출 | 토큰 폭주 시 사용자 비용 | V1.5 rate limit + per-user 토큰 cap |
| 첫 출하까지 4.5~5주 | 빠른 검증 어려움 | walking skeleton, wipe 라 4주 미만 무리 |
| 추상 비용 V1 LOC +400 | 일정 부담 | V1.5/V2 에서 ~1,500 LOC 절감 (흑자) |
| 시멘틱 충돌 해결 UI 부재 (V1) | admin 이 main 바꿔도 channel override 우선 — 사용자 혼선 가능 | V1.5 `/semantic diff` + 알림 |
| 의미 추출 LLM 환각 | 문서에서 잘못된 metric 추출 | 사용자 confirm 1차 방어, V1.5 검증 layer |
| 메모리 안 recall (V1) | facts 많아지면 토큰 폭증 | V1.5 키워드 recall, V2 벡터 recall |
| frontend 어댑터 추가 비용 | Slack/Web 각 ~1,000 LOC | Phase 2/3 단계 적용 |

## 5.3 비교 — 기존 오픈소스 대비 위치

| 영역 | Vanna AI | Wren AI | SQLCoder | **Lang2SQL (우리)** |
|---|---|---|---|---|
| 자연어 → SQL | ✅ RAG | ✅ MDL | ✅ fine-tuned | ✅ RAG + 시멘틱 + harness |
| DB 직접 연결 | ✅ | ✅ | ✅ | ✅ |
| Semantic layer | ❌ | ✅ (수동 MDL) | ❌ | ✅ (**문서 자동 추출**) |
| **시멘틱 federation (팀별 분기)** | ❌ | ❌ | ❌ | ✅ (**git-like 3-scope**) |
| **DB 메타데이터 자동 보강** | ❌ | ❌ | ❌ | ✅ (V1.5 보강 layer) |
| 대화 기억 | ❌ | ❌ | ❌ | ✅ (Hermes) |
| 멀티유저 협업 | 2.0 추가 | enterprise BI | ❌ | ✅ (Discord thread, frontend 추상) |
| Frontend 종류 | Web UI | Web UI | CLI | **Discord/Slack/Web (어댑터)** |
| 확장 모델 | 단일 아키텍처 | enterprise platform | 단일 LLM | **4가지 패턴 (chain/strategy/scope)** |

**위치 요약**: *Vanna 의 자연어→SQL + Wren 의 시멘틱 + 우리만의 (문서 자동 추출 + git-like 팀 federation + DB/시멘틱 두 축 강건성 + Hermes 기억 + 멀티 인터페이스)*.

## 5.4 한 줄

> **"문서를 넣어 비즈니스 맥락을 학습시키고, 팀별로 시멘틱이 분기되고, DB가 불완전해도 견디고, 모든 정의·대화를 기억하는 오픈소스 분석 에이전트. Phase 1 은 Discord, 그 다음은 Slack·Web."**

---

# 결정 필요 사항

V1 착수 전 답이 있으면 좋은 항목:

1. **첫 배포 Discord 길드** — 본인 테스트 / 가짜연구소 / 기타?
2. **OpenAI API 키** — 본인 / 팀?
3. **레포 이름** — `lang2sql/` 유지 + 새 브랜치 (`feature/v4-rebuild`) 권고
4. **첫 데모 시나리오** — bench/ 에 들어갈 *완성된 use case* 1개. 예: "이커머스 매출 분석 — 4 테이블, 3 metric, 1 문서, 5개 질문, 마케팅 채널 vs 파이낸스 채널에서 다른 정의로 같은 질문"
5. **`/define_metric` 디폴트 scope** — channel (권고) vs DM 발생 시 personal vs admin 명시 강제?
6. **NIM 도입 시점** — V1.5 contract test 후 / V2 보류?

답해 주시면 v4.1-final 마무리하고 Week 1 부터 시작.

— end —
