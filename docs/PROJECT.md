# Lang2SQL — 프로젝트 SSOT

> *"질문하면 SQL 짜주는 봇이 아니라, 현실의 messy함에 견디는 분석 에이전트."*

이 문서는 이 프로젝트가 *무엇이고, 왜 존재하며, 지금 어디까지 와 있는지*를 **단일하게** 설명합니다. 다른 모든 문서·README·디자인노트는 이 문서를 참조하거나 보충합니다.

---

## 1. 한 줄 정체성

**Lang2SQL**은 *문서로 비즈니스 맥락을 학습하고, 팀별로 시멘틱이 분기되고, 불완전한 DB에서도 답하고, 모든 정의·대화를 기억하는* 오픈소스 분석 에이전트입니다. Phase 1 인터페이스는 **Discord**.

---

## 2. 왜 존재하는가

Vanna AI(~20k★), Wren AI(~12k★), SQLCoder 같은 Text-to-SQL 오픈소스들은 *질문→SQL 파이프라인* 자체는 이미 잘 풉니다. "더 좋은 SQL 생성"은 모델 fine-tuning 싸움이고, 그 영역엔 들어가지 않습니다.

대신 *실무에 넣어보면 진짜 막히는* **현실의 지저분함 4가지**를 다룹니다:

| 약점 | 기존 처리 | 우리 해결 |
|---|---|---|
| DB 메타데이터가 비어 있다 | Vanna: 학습 데이터 의존 | ★① **DB 강건성**: 연결 즉시 후보 보강 + 이후 사람 검토 |
| 봇이 어제 한 얘기를 못 기억한다 | 대부분 stateless | ★② **Hermes 기억**: 3축 분리(Store/Recall/Extractor) |
| 비즈니스 정의를 사람이 일일이 입력 | Wren: MDL 수동 | ★③ **Ingestion 매트릭스**: 문서 → 시멘틱 후보 |
| 같은 *"활성 사용자"*가 팀마다 다르다 | Wren: 단일 MDL → 충돌 | ★④ **Semantic federation**: git-like 분기, 가장 구체적 scope 승리 |

이 4가지는 *비즈니스마다 다르기 때문에 벤치마크가 안 나오는 영역* → 그래서 오픈소스가 안 건드림 → **그래서 기회**.

---

## 3. 무엇을 다르게 하는가 — 4기둥

| 기둥 | 한 줄 | 자세히 |
|---|---|---|
| **★① Safety pipeline** | 모든 SQL이 통과해야 하는 *공항 보안 검색대* | layer를 줄 세우는 패턴 — 새 검사(예: AST 검증, 함수 차단)는 한 칸 끼우기 |
| **★② Memory 3축** | Store/Recall/Extractor 각각 독립 진화 | V1엔 in-memory/inject-all/manual, V1.5엔 SQLite/keyword/auto |
| **★③ Ingestion matrix** | Source × Extractor 자유 조합 | 파일×LLM이 V1, URL/Notion/DDL은 V1.5+ |
| **★④ Semantic federation** | git처럼 팀별 정의 분기, *가장 구체적이 승리* | 충돌이 사라짐. Wren의 "한 회사 한 MDL"이 못 푸는 영역 |

**핵심 메타원칙**: 모든 외부 시스템 의존성을 *포트(Protocol)*로 추상화. *어댑터*는 가장자리에만. 그래서 새 LLM / 새 DB / 새 frontend 추가가 *기존 코드 안 건드리고 끼우기*로 끝남.

검토형 질의는 기존 4기둥을 사용하는 연결별 추가 실행 모드다. Enrich·Federation이
발견·축적한 의미 후보를 현재 질문에서 안전하게 실행할 typed plan으로 제한한다.

---

## 4. 지금 어디까지 와 있는가 — 정직한 현황

### ✅ 현재 구현
- **core 포트 11종** — 모든 외부 의존을 Protocol로 추상화
- **harness** — agent_loop(LLM → tool → 다음 턴), Session, HarnessContext
- **★①~★④ 4기둥** 최소 구현 — safety 12 회귀, memory 3축, ingestion 매트릭스, federation 3-scope
- **두 질의 도구 조립 모드** — catalog가 없는 연결은 기존 `run_sql`·schema
  탐색/보강, catalog가 활성화된 연결은 SQL 없는 `semantic_query`. Federation,
  ingestion, memory는 등록·직접 명령 경로로 유지하지만 검토형 자연어 turn에서
  모델에 노출되는 도구는 `semantic_query`와 `ask_user`뿐
- **Discord 프론트엔드** — 슬래시 명령 + `/setup` 위저드 (비개발자 DSN-free flow) + bot.py
- **연결 즉시 의미 준비** — DB comment를 후보로 사용하고, 같은 source·물리
  fingerprint의 재연결은 기존 Enrich 설명 캐시도 재사용한다. `auto` 모드에서 실제
  provider가 설정되면 metadata-only LLM 보강을 자동 1회 수행. 후보는 질문에서
  필요할 때만 사람 검토
- **영속화** — KV store(federation) + Fernet 실암호화 secrets
- **DB 어댑터** — `SqlAlchemyExplorer`의 DSN 라우팅 + Cloudflare D1 HTTP 어댑터.
  검토형 compiler·실행 계약은 기존 file-backed SQLite/DuckDB에서 검증
- **357개 자동화 테스트** (safety, semantic runtime, 공개 API, Discord 포함)
- **bench 데모** — federation + safety 라이브 시연 (`bench/ecommerce_demo.py`)

### ⚠️ Stub / 미검증
| 항목 | 상태 |
|---|---|
| PostgreSQL 실 연결 | psycopg 어댑터는 있음. 실 PG 테스트 미수행 |
| 원격 DB의 검토형 실행 | connector 지원과 별개. 현재 SQLite/DuckDB 외 dialect는 fail-closed |
| raw-value Enrich와 검토형 catalog의 더 깊은 통합 | 현재는 기존 설명 캐시를 후보로만 재사용 |
| 키워드/벡터 recall | V1.5/V2 |
| LLM 자동 fact 추출 | V1.5 |
| `/semantic diff`, `/semantic promote` | V1.5 |
| URL/Notion 문서 입력 | V1.5/V2 |
| Slack/Web frontend | Phase 2/3 |
| Audit hash chain | V2 |

---

## 5. 로드맵

```
V1   ✅  골격 + 4기둥 최소 + Discord 어댑터 + 영속화        ← 지금
V1.5 →  후보 alias를 넘어선 설명·카드 보강(★①) + 키워드 recall +
         LLM 자동 fact 추출 + /semantic diff·promote +
         URL/DDL ingestion + 회귀 강화
V2   →  벡터 recall + 비용 게이트(EXPLAIN) + Notion MCP +
         외부 git semantic 동기화 + Slack frontend
V2.5 →  PostgreSQL 멀티인스턴스 + branch fork/merge UI +
         Web frontend
```

각 단계의 디테일은 [`docs/discord_first_redesign_v4_1.md`](./discord_first_redesign_v4_1.md) §3.

---

## 6. 빠른 시작

```bash
git clone https://github.com/CausalInferenceLab/Lang2SQL.git
cd Lang2SQL
uv sync --extra duckdb                   # 검토형 SQLite/DuckDB 경로 포함
.venv/bin/pytest -q                      # 전체 회귀
.venv/bin/python bench/ecommerce_demo.py # federation + safety 데모
```

Discord 봇 운영: [`docs/DEPLOY.md`](./DEPLOY.md)

---

## 7. 아키텍처 & 기여

- **아키텍처 한눈 가이드 + 어디 손대면 좋은지**: [`docs/ARCHITECTURE.md`](./ARCHITECTURE.md)
- **PR 작성 형식**: [`docs/pull_request_guidelines.md`](./pull_request_guidelines.md)
- **브랜치 전략**: [`docs/branch_guidelines.md`](./branch_guidelines.md)

기여 PR을 가장 받기 쉬운 지점들 (자세한 위치/방법은 ARCHITECTURE.md §5):
- 새 LLM 어댑터 (`adapters/llm/<provider>.py`)
- 새 safety layer (`safety/layers/<name>.py`)
- 새 memory recall 전략 (`memory/recall/<name>.py`)
- 새 ingestion source (`ingestion/sources/<name>.py`)
- 새 frontend (`frontends/<platform>/`)
- 새 도구 (`tools/<name>.py`)

---

## 8. 핵심 설계 결정 (왜 이 길을 택했나)

| 결정 | 이유 |
|---|---|
| **백지 재작성** (LangGraph/Streamlit 파이프라인 → ports & adapters 에이전트) | 파이프라인 위에 4기둥을 얹는 것보다, 4기둥을 *전제*로 새로 짓는 게 깔끔함 |
| **포트 & 어댑터** (콘센트와 가전) | V1엔 단순 구현 1개씩, 어댑터 추가는 기존 코드 안 건드림. *연구실 코드가 곧 제품 layer* |
| **Discord 1급 frontend, 나머지는 추상** | "오픈소스 분석 봇"의 자연스러운 거주지. Slack/Web은 어댑터 추가 |
| **"강건성"을 두 축으로 분리** (DB ★① + 시멘틱 ★④) | 실무에선 후자가 더 자주 터진다는 발견. 학계엔 ★①, 거버넌스엔 ★④ |
| **federation = git-like 분기** | "한 회사 한 정의"는 조직 현실과 충돌. 각자 scope에서 살게 함 |
| **Read-only를 fail-closed로 강제** | safety pipeline의 whitelist는 SELECT/WITH 외 BLOCK. DROP/INSERT가 모델 환각으로 새는 사고 방지 |
| **stdlib → 필요 시 lean dep** | 초기엔 의존성 0(urllib OpenAI 어댑터), V1.5에서 cryptography/discord.py만 핀 |

---

## 9. 프로젝트 메타

- **License**: [MIT](https://opensource.org/licenses/MIT)
- **운영**: [가짜연구소](https://pseudo-lab.com/) 인과추론팀
- **커뮤니티**: [Discord](https://discord.gg/EPurkHVtp2)
- **이슈/기능 요청**: [GitHub Issues](https://github.com/CausalInferenceLab/Lang2SQL/issues)
- **백업**: 옛 v0.3 아키텍처는 `archive/pre-v4.1-rebuild` 태그로 복원 가능

---

## 10. 변천 (간단)

| 시기 | 사건 |
|---|---|
| ~v0.3 | LangGraph + Streamlit 파이프라인 (질문→retrieval→gate→generation→execution) |
| 2026 봄 | **방향 전환**: Vanna/Wren도 이미 잘 푸는 영역에서 경쟁 그만, "현실 robustness"로 이동 |
| 2026-05 | v4.1 plan 확정 → ports & adapters로 백지 재작성 (PR #227–#230) |
| (지금) | 연결 즉시 후보 보강 구현. 다음은 V1.5 — 설명·카드와 사람 피드백의 더 깊은 통합 |

— *"더 똑똑한 SQL 생성기가 아니라, 현실의 messy함에 견디는 도구."*
