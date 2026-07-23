# Lang2SQL

<div align="center">
<a href="https://pseudo-lab.com"><img src="https://img.shields.io/badge/PseudoLab-S10-3776AB" alt="PseudoLab"/></a>
<a href="https://discord.gg/EPurkHVtp2"><img src="https://img.shields.io/badge/Discord-BF40BF" alt="Discord Community"/></a>
<a href="https://github.com/CausalInferenceLab/lang2sql/stargazers"><img src="https://img.shields.io/github/stars/CausalInferenceLab/lang2sql" alt="Stars Badge"/></a>
<a href="https://github.com/CausalInferenceLab/lang2sql/network/members"><img src="https://img.shields.io/github/forks/CausalInferenceLab/lang2sql" alt="Forks Badge"/></a>
<a href="https://github.com/CausalInferenceLab/lang2sql/pulls"><img src="https://img.shields.io/github/issues-pr/CausalInferenceLab/lang2sql" alt="Pull Requests Badge"/></a>
<a href="https://github.com/CausalInferenceLab/lang2sql/issues"><img src="https://img.shields.io/github/issues/CausalInferenceLab/lang2sql" alt="Issues Badge"/></a>
<a href="https://github.com/CausalInferenceLab/lang2sql/graphs/contributors"><img alt="GitHub contributors" src="https://img.shields.io/github/contributors/CausalInferenceLab/lang2sql?color=2b9348"></a>
</div>

<p align="center">
  <strong>우리는 함께 코드와 아이디어를 나누며 더 나은 데이터 환경을 만들기 위한 오픈소스 여정을 떠납니다. 🌍💡</strong>
</p>

---

> **A document-learning, read-only SQL analytics agent.**
> Feed it your company's docs → it learns your business context → it keeps a
> *separate* set of definitions per team → it answers questions over an
> incomplete database → it remembers every definition and conversation.

> **이번 변경의 위치:** 기존 ContextFlow를 대체하지 않는다. Enrich, Semantic
> federation, Memory, Discord 흐름은 유지하고, catalog가 활성화된 연결의 **DB 질의
> 실행 경계**만 검토형 typed plan으로 강화한다.

👉 **프로젝트 전체 그림(단일 SSOT)**: [`docs/PROJECT.md`](docs/PROJECT.md) · **컨트리뷰터 한눈 가이드**: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)

This is the **v4.1 rebuild** (배경/설계 의도: [`docs/discord_first_redesign_v4_1.md`](docs/discord_first_redesign_v4_1.md)).
Where most text-to-SQL projects compete on *"generate better SQL,"* Lang2SQL
competes on everything *around* the query: business-context learning, per-team
semantics, robustness to messy databases, and memory. **Discord is the Phase 1
interface, not the identity** — Slack/Web are adapters on the same core.

---

## The four pillars

| Pillar | What it is |
|---|---|
| **① Business-context learning** | Documents are the source of truth. Drop in a doc → the agent extracts metric/dimension/rule candidates → you confirm → they land in the semantic layer. |
| **② Two-axis robustness** | **(2a) DB robustness** — starts from physical metadata and candidate-only enrichment even when descriptions are incomplete. **(2b) Semantic robustness** — teams hold *different* definitions of the same term without conflict. This axis is the product/research identity. |
| **③ Hermes memory** | Conversations, facts, and preferences persist instead of resetting each session. |
| **④ Multi-interface** | Phase 1 Discord today; Slack/Web are future adapters. No platform lock-in. |

### 이번 PR: 연결 즉시 의미 준비형 질의

ContextFlow의 Enrich·Federation·Memory는 의미를 발견하고 축적한다. 이번 PR은
그 구조를 교체하지 않고, **semantic catalog(연결별 허용 의미 목록)가 활성화된
연결의 실행 경계**를 추가한다. 서버가 연결 시 catalog를 만들고 질문마다 후보를
제한하면, 모델은 SQL 대신 허용된 metric/dimension ID·집계와 질문에서 가져온
filter·기간만 선택한다. 이 PR에는 catalog 생성·검토·컴파일·실행 코어, 공개 API,
Discord 관리 UX와 SQLite/DuckDB 검증이 함께 포함된다.

#### 1. 기존 시스템에서 무엇이 달라졌나

<p align="center">
  <a href="docs/assets/contextflow-semantic-query-delta.svg">
    <img src="docs/assets/contextflow-semantic-query-delta.svg" width="100%" alt="기존 ContextFlow에서 유지되는 기능과 이번 PR이 추가한 검토형 실행 경계"/>
  </a>
</p>
<p align="center"><sub>그림을 누르면 원본 크기로 볼 수 있다.</sub></p>

| 영역 | Catalog 없는 기존 연결 | Catalog가 활성화된 연결 |
|---|---|---|
| 모델 질의 도구 | `run_sql`, Explore, Enrich | `semantic_query`와 `ask_user`만 노출 |
| 모델 출력 | SQL 문자열 | metric/dimension ID, 집계, filter, 기간 |
| Enrich | row sample 기반 보강 가능 | 모델 도구에서는 제외; 같은 source/fingerprint의 기존 설명만 후보로 재사용 |
| 의미 확정 | 모델과 기존 의미 계층에 의존 | 업무 표현·집계·분류 공개 범위를 서로 분리해 사람 검토 |
| 실행 | 기존 Safety pipeline | catalog/source 재검증 → 서버 SQL 컴파일 → 기존 Safety |
| 실패 처리 | legacy 질의 경로 | raw SQL로 우회하지 않고 추가 질문·검토·차단 |

Memory·Ingestion·Federation의 저장 기능과 Discord 인터페이스는 그대로 남는다.
다만 catalog가 활성화된 **자연어 DB 질문 turn**에서는 모델이 raw SQL·Explore·
Enrich를 호출하지 못한다.

#### 2. 연결할 때 catalog를 어떻게 만드는가

<p align="center">
  <a href="docs/assets/semantic-catalog-build.svg">
    <img src="docs/assets/semantic-catalog-build.svg" width="100%" alt="DB metadata에서 semantic catalog를 만들고 연결별 상태로 관리하는 과정"/>
  </a>
</p>
<p align="center"><sub>그림을 누르면 원본 크기로 볼 수 있다.</sub></p>

1. **메타데이터만 읽기(metadata scan)**
   - table·column·type·nullability·PK/FK·DB comment만 읽는다.
   - 연결 시 raw row, 범주 값 목록, PII 값을 sample하지 않는다.

2. **물리 column 분류**
   - PII·credential·free text·key·비지원 type은 `blocked_columns`로 보낸다.
   - numeric measure는 `MetricSpec`, time/boolean/categorical은
     `DimensionSpec` 후보로 만든다.
   - 각 table에는 물리적인 source-record `COUNT(*)` metric을 별도로 만든다.
   - 일반 numeric column은 발견 즉시 업무 지표로 승인하지 않는다.

3. **Join과 검색 표현 후보 생성**
   - 선언된 단일-column FK의 child → parent 방향만 join 후보로 등록한다.
   - physical name과 DB comment를 후보 표현(alias)으로 만든다.
   - 동일 source와 동일 스키마 지문(fingerprint)의 재연결에서만 기존 Enrich 설명과
     사람 검토 결정을 이어받는다.
   - `LANG2SQL_AUTO_METADATA_ENRICH=auto`와 실제 provider가 설정됐거나 `llm`
     모드를 명시하면 metadata-only alias 보강을 한 번 수행한다.
   - 충돌 alias·값 목록형 표현·URL/email/SQL형 문자열은 제거한다. LLM 보강
     결과도 승인된 업무 의미, join, 집계, 공개 권한이 아니다.

4. **정규화·식별·원자적 활성화**
   - 물리 schema snapshot을 정렬한 뒤 SHA-256 fingerprint를 만든다.
   - encrypted credentials, catalog JSON, connection binding을 하나의 SQLite
     transaction으로 활성화한다.
   - 세 상태가 어긋나거나 catalog가 손상되면 legacy `run_sql`로 돌아가지 않는다.

재연결·schema 변경·검토 변경은 서로 다른 상태 값으로 추적한다. 현재 상태와 맞지
않는 이전 후보와 draft는 재사용하지 않는다.

<details>
<summary><strong>Catalog 상태 필드 자세히 보기</strong></summary>

| 상태 | 의미 | 바뀌는 시점 |
|---|---|---|
| `source_id` | scope와 canonical DSN/extras를 비가역적으로 묶은 실행 source identity | DB·credential·연결 option 변경 |
| `connection_generation` | 현재 활성 연결 세대 | 재연결할 때마다 |
| `fingerprint` | 물리 schema snapshot의 지문 | 재연결 scan에서 schema 변경 감지 |
| `review_revision` | 사람 검토 결정의 동시성 marker | 표현·집계·공개 상태 변경 |
| catalog/policy version | 저장 형식과 분류·shortlist 규칙 버전 | 코드 정책이 변경될 때 |

`catalog.version`은 내용이 바뀔 때마다 증가하는 revision이 아니다. 물리 변경은
`fingerprint`, 사람 결정 변경은 `review_revision`으로 구분한다. Discord에서는
`/semantic_status`로 현재 후보·보강·검토 상태를 확인할 수 있다.

</details>

#### 3. 작은 모델이 실제로 하는 일

- **후보 검색은 서버가 수행한다.** 현재 구현은 vector search가 아니라 physical
  name·승인 alias·후보 alias를 이용한 문자열 기반 검색이다.
- **입력 크기를 제한한다.** table 6개, metric 12개, dimension 12개, tool schema
  12 KiB 이내로 제한한다. 작은 catalog는 상한 안의 전체 후보를 줄 수 있고,
  넓은 catalog는 질문에 정확히 나타난 metadata 표현으로 좁힌다.
- **모델은 typed slot만 채운다.** metric/dimension ID, 허용 집계, 질문에서 복사한
  filter 값과 기간, 미지원 요구사항을 구조화한다. SQL·table·join·dialect는
  선택하지 않는다.
- **서버가 다시 검증한다.** candidate token은 질문·사용자·대화·source·연결
  세대에 묶고, plan 단계에서 현재 catalog/revision으로 shortlist와 정책을
  다시 검사한다.
- **미확정 의미는 실행하지 않는다.** 같은 요청자가 15분 안에 검토하면 exact
  draft를 재검증해 이어갈 수 있다. 다른 관리자 승인·만료·서버 재시작 뒤에는
  질문을 다시 제출해야 한다.

```text
질문       status가 paid인 주문의 amount 합계

서버 후보  metric:orders.amount
           dimension:orders.status
           aggregate: SUM · operator: EQ

모델 출력  metric_id=metric:orders.amount
           aggregate=SUM
           filter.dimension_id=dimension:orders.status
           operator=EQ · value="paid"

서버 처리  현재 catalog/review/source 확인
           → 유일한 안전 FK·bound parameter로 SQL 컴파일
           → Safety·결과 공개 정책
           → read-only 실행 또는 명시적 차단
```

작은 모델의 성능을 높이는 핵심은 더 많은 추론을 요구하는 것이 아니라,
**연결 시점에 후보를 준비·관리하고 질문 시점에 선택 문제로 축소하는 것**이다.

#### 4. 현재 검증 경계

- 업무 의미를 metadata만으로 완전히 복원한다고 주장하지 않는다. 사람 피드백은
  catalog에 저장되고 같은 source/fingerprint에서만 재사용된다.
- 문자열 기반 후보 검색이며 vector/VDB recall은 아직 구현하지 않았다.
- 모델이 자연어의 숨은 조건을 전혀 발견하지 못하는 planner 문제는 별도 평가 대상이다.
- 검토형 compiler·timeout/cancel·read-only 실행의 현재 근거는 기존 file-backed
  SQLite와 DuckDB다. 다른 connector의 연결 가능성과 안전 실행 검증은 구분한다.

상세 실행 계약은 [`연결 즉시 의미 준비형 질의`](docs/REVIEWED_SEMANTIC_QUERY.md),
호스트 통합 API는 [`LIBRARY_API`](docs/LIBRARY_API.md), Discord 운영 절차는
[`USAGE`](docs/USAGE.md)를 참고한다.

## Extensibility — outlets and appliances (콘센트/가전)

V1 ships the **simplest single implementation** of each extension point, but the
**abstraction (port) is already in place**, so v1.5/v2 add a new implementation
*without touching existing code*. Like a wall outlet: the V1 socket has one LED
bulb plugged in, but because the socket is standard, you later plug in a fan or a
smart light without rewiring the wall.

Four ★ extension patterns sit behind `core/ports/`:

| ★ | Pattern | Port | Grows by |
|---|---|---|---|
| ① | **Safety pipeline** | `ports/safety.py` | adding one layer class to the line (zero `run_sql` changes) |
| ② | **Memory service** | `ports/memory.py` | swapping any of 3 axes — Store / Recall / Extractor — independently |
| ③ | **Ingestion pipeline** | `ports/ingestion.py` | a Source × Extractor matrix |
| ④ | **Semantic federation** | `ports/semantic_scope.py` | git-like per-team scope branches |

Everything outside `tenancy/concierge.py` depends only on these Protocols, so the
concrete classes (OpenAI, Postgres, SQLite) are swappable at the seams.

---

## Quickstart

Requires Python ≥ 3.10 and [uv](https://docs.astral.sh/uv/).

```bash
uv sync                       # create .venv and install deps
# DuckDB 파일도 연결할 때만 선택 dependency를 함께 설치
uv sync --extra duckdb
```

### 1. Run the offline demo (no token, no database)

```bash
.venv/bin/python bench/ecommerce_demo.py
```

Shows the federation money-shot (one term, two team definitions, no conflict) and
the safety gate (DROP/INSERT blocked, SELECT passes). See [`bench/README.md`](bench/README.md).

### 2. Run the CLI (developer driver)

```bash
.venv/bin/lang2sql "list the tables"
```

The CLI assembles a real `HarnessContext` and runs one turn through the agent
loop. With `OPENAI_API_KEY` set it calls `gpt-4.1-mini`; otherwise it uses the
offline `FakeLLM`.

### 3. Run the Discord bot

```bash
export DISCORD_BOT_TOKEN=...        # required
export OPENAI_API_KEY=...           # required for real answers, unless using local model below
export LANG2SQL_SECRET_KEY=...      # optional; Fernet key for secret encryption
# optional: parent channel IDs where non-admin members may query
export LANG2SQL_DISCORD_QUERY_CHANNEL_IDS=123456789012345678
.venv/bin/lang2sql-bot
```

Local OpenAI-compatible alternative:

```bash
export LANG2SQL_LLM_BASE_URL=http://127.0.0.1:11434
export LANG2SQL_LLM_MODEL=gemma4:26b
```

With neither provider configured, `FakeLLM` is only an installation smoke and
does not provide a meaningful semantic-query experience.

The bot exits loudly if `DISCORD_BOT_TOKEN` is unset. Full setup and hosting:
[`docs/DEPLOY.md`](docs/DEPLOY.md). Copy [`.env.example`](.env.example) to start.

### 앱에 내장하기: 모델이 SQL을 작성하지 않는 공개 API

Discord 외의 앱은 `Lang2SQLRuntime`의
`connect → candidates → feedback → plan → execute` 흐름을 사용할 수 있다.
호스트와 모델은 SQL 문자열을 만들거나 받지 않는다. DTO와 검토 루프는
[`docs/LIBRARY_API.md`](docs/LIBRARY_API.md)에 있다.

```bash
uv run python examples/semantic_runtime_quickstart.py
```

---

## What V1 does / does NOT do yet (honesty section)

**Does:**
- 3-scope semantic federation (guild / channel / member) with most-specific-wins
  resolution; `term_custom` registers definitions per scope (KV-backed).
- Safety pipeline with the V1 layers (whitelist + timeout), gating every query.
- Legacy raw mode includes `run_sql`, schema exploration/enrichment, semantic
  term, ingestion, memory, and clarification tools.
- 연결 즉시 의미 준비형 질의 모드는 catalog가 활성화된 연결에서 `run_sql`과
  sample-based schema exploration을 `semantic_query`로 교체한다.
- Memory service (in-memory store + inject-all recall + manual `/remember`).
- Discord frontend (bot, commands, session router, render).
- Encrypted-at-rest secrets (Fernet) and SQLite-backed persistence.
- Connect-time candidate enrichment from DB comments, the existing Enrich
  cache, and an optional metadata-only LLM pass; lazy business-meaning review;
  and a typed aggregate/group-by path over declared many-to-one FK paths.
- Private-by-default aggregate disclosure: fewer than five contributing rows
  blocks `SUM`/`AVG`/source-record `COUNT`, while `MIN`/`MAX` require an explicit
  public-data scope with no controlled dimension.

**Does NOT yet:**
- **Replace the no-DB default fixture.** If neither `LANG2SQL_DB_URL` nor
  Discord `/setup` supplies a connection, the canned `PostgresExplorer` remains
  the offline demo. Setup connections themselves execute through SQLAlchemy or
  D1 when their drivers and network are available.
- **Reason without a configured model.** Without `OPENAI_API_KEY` or the local
  model variables, `FakeLLM` returns deterministic canned tool cycles — useful
  for wiring tests, not for answers.
- Turn candidate enrichment into approved descriptions, business formulas, or
  inferred joins automatically. Richer semantic cards and feedback-driven
  enrichment, AST-precise SQL validation, function blocklists, cost gating,
  `/semantic diff` / `/semantic promote`, keyword/vector recall, automatic fact
  extraction, and URL/Notion ingestion remain v1.5+.
- Persist across restarts by default: the V1 `SqliteStore` defaults to in-memory;
  point it at a file for durability.
- Silently widen the typed-query boundary: advanced filters (OR/NOT, partial
  match, free search), timestamp/relative/fiscal/cohort time, unit conversion,
  derived formulas, composite joins, and fan-out joins fail closed rather than
  being guessed or dropped.
- Claim universal dialect parity: current public reviewed-execution evidence
  covers existing file-backed SQLite and DuckDB only. Connector availability and
  verified reviewed execution are reported separately; unverified remote
  dialects fail closed.

---

## Roadmap at a glance

| Area | V1 | V1.5 | V2 | V2.5 |
|---|---|---|---|---|
| **Safety** | whitelist + timeout | + AST validation, function blocklist, auto LIMIT, **richer semantic cards**, rate limit | + cost gate (EXPLAIN), per-engine pipelines | — |
| **Memory** | in-memory + inject-all + manual | SQLite store + keyword recall + auto-extract | + vector recall + conflict resolution | PostgreSQL + hybrid recall + confidence |
| **Ingestion** | file upload + LLM extract | + URL fetch + DDL parsing | + Notion/Confluence + hybrid | + GitHub/Drive + chunked RAG |
| **Federation** | 3-scope resolution, `/semantic show` | `/semantic diff`, `/semantic promote`, conflict alerts | git sync (semantic-as-code) | branch fork/merge UI, per-scope audit |
| **Interface** | Discord | (Anthropic/NIM eval) | Slack | Web |

See [`docs/discord_first_redesign_v4_1.md`](docs/discord_first_redesign_v4_1.md)
for the full architecture write-up.

---

## 🤝 기여하기

**처음 보시는 분은 [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)** — 디렉토리·레이어 책임, 한 메시지의 lifecycle, *어디를 수정하면 좋을지* 가 한곳에 정리돼 있습니다.

```bash
git clone https://github.com/CausalInferenceLab/lang2sql.git
cd lang2sql
uv sync --extra duckdb
uvx ruff check src/lang2sql tests bench/local_model_semantic_eval.py examples/semantic_runtime_quickstart.py
uv run mypy src/lang2sql examples/semantic_runtime_quickstart.py
uv run pytest -q
```

CI는 Python 3.10과 3.12에서 같은 검사를 실행하며 DuckDB 실행 경로를 필수로 확인한다.

- 새 기능에는 테스트 작성 (`tests/test_<layer>.py`)
- PR은 `master` 브랜치 대상, 커밋 메시지에 `feat:` / `fix:` / `docs:` prefix 사용
- 버그/기능 요청은 [이슈](https://github.com/CausalInferenceLab/lang2sql/issues)로

---

## 🙏 감사의 말 / License

Lang2SQL은 **가짜연구소의 인과추론팀**에서 개발 중인 프로젝트입니다. Licensed under
the [MIT License](https://opensource.org/licenses/MIT). 커뮤니티: [Discord](https://discord.gg/EPurkHVtp2).

---

## 🏆 Our Team

| Role | Name | Skills | Interests |
|------|------|--------|-----------|
| **Project Manager** | 이동욱 | ![Python](https://img.shields.io/badge/Python-Expert-3776AB) | LLM, Open Source, Causal Inference |
| **AI Engineer** | 문찬국 | ![Python](https://img.shields.io/badge/Python-Expert-3776AB) | LLM, Agentic RAG, Open Source |
| **Data Engineer** | 박경태 | ![Python](https://img.shields.io/badge/Python-Expert-3776AB) | LLM-driven Data Engineering |
| **AI Engineer** | 손봉균 | ![Python](https://img.shields.io/badge/Python-Expert-3776AB) | LLM, RAG, AI Planning |
| **Data Scientist** | 안재일 | ![Python](https://img.shields.io/badge/Python-Intermediate-FF6C37) | LLM, Data Analysis, RAG |
| **ML Engineer** | 이호민 | ![Python](https://img.shields.io/badge/Python-Expert-3776AB) | Multi-Agent Systems |
| **AI Engineer** | 최세영 | ![Python](https://img.shields.io/badge/Python-Expert-3776AB) | LLM, RAG, Multi-Agent |
| **Full-Stack Developer** | 황윤진 | ![NextJs](https://img.shields.io/badge/NextJs-Expert-3776AB) ![React](https://img.shields.io/badge/React-Expert-3776AB) | LLM Orchestration |
| **AI Engineer** | 김경서 | ![Python](https://img.shields.io/badge/Python-Expert-3776AB) | LLM, FinNLP, FDS, RAG |
| **Data Engineer** | 홍지영 | ![Python](https://img.shields.io/badge/Python-Expert-3776AB) | LLM, Data Engineering |
| **Data Operator** | 이화림 | ![Python](https://img.shields.io/badge/Python-Expert-3776AB) | LLM, Data Engineering |
| **AI Engineer** | 남경혜 | ![Python](https://img.shields.io/badge/Python-Expert-3776AB) | LLM, RAG, Multi-Agent |
| **AI Engineer** | 심세원 | ![Python](https://img.shields.io/badge/Python-Expert-3776AB) | LLM, RAG, Multi-Agent |
| **Business Analyst** | 서희진 | ![Python](https://img.shields.io/badge/Python-Intermediate-FF6C37) | LLM, Data Analysis |

---

## 🌍 가짜연구소 소개

[가짜연구소](https://pseudo-lab.com/)는 머신러닝과 AI 기술 발전에 중점을 둔 비영리 조직입니다. **공유, 동기부여, 그리고 협업의 기쁨**이라는 핵심 가치를 바탕으로 영향력 있는 오픈소스 프로젝트를 만들어갑니다.

전 세계 5,000명 이상의 연구자들과 함께, 우리는 AI 지식의 민주화와 열린 협업을 통한 혁신 촉진에 전념하고 있습니다.

**커뮤니티**: 💬 [Discord](https://discord.gg/EPurkHVtp2)

---

## 🎯 기여자들

<a href="https://github.com/CausalInferenceLab/lang2sql/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=CausalInferenceLab/lang2sql" />
</a>
