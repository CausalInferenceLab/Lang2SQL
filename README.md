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

연결된 DB에 **governed semantic mode**가 활성화되면 모델은 SQL을 작성하지 않는다.
모델은 사람이 검토한 지표·분류·필터 후보만 고르고, 결정론적 코드가 검증된 SQL을
컴파일한다.

👉 **프로젝트 전체 그림(단일 SSOT)**: [`docs/PROJECT.md`](docs/PROJECT.md) · **컨트리뷰터 한눈 가이드**: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)

This is the **v4.1 rebuild** (배경/설계 의도: [`docs/discord_first_redesign_v4_1.md`](docs/discord_first_redesign_v4_1.md)).
Where most text-to-SQL projects compete on *"generate better SQL,"* Lang2SQL
competes on everything *around* the query: business-context learning, per-team
semantics, robustness to messy databases, and memory. **Discord is the Phase 1
interface, not the identity** — Slack/Web are adapters on the same core.

### 처음 읽을 때 필요한 용어

| 용어 | 이 프로젝트에서의 뜻 |
|---|---|
| **Semantic catalog** | 테이블·컬럼·PK/FK 같은 물리 사실과 사람이 확인한 업무 표현·집계·공개 정책을 함께 보관하는 연결별 카탈로그 |
| **Candidate** | 모델이나 UI가 고를 수 있도록 서버가 질문별로 제한한 지표·분류·필터·기간 후보 |
| **Human review** | 불확실한 업무 의미나 데이터 공개 범위를 사람이 명시적으로 승인·거절하는 단계 |
| **Typed plan** | SQL 문자열 대신 metric, aggregate, group-by, filter, date 범위를 구조화한 검증 대상 |
| **Governed mode** | semantic catalog가 활성화되어 모델의 `run_sql` 접근을 제거하고 `semantic_query`만 허용하는 모드 |
| **Fail-closed** | 의미·권한·join·dialect를 확신할 수 없을 때 추측하거나 raw SQL로 우회하지 않고 질문 또는 차단으로 끝내는 동작 |

---

## The four pillars

| Pillar | What it is |
|---|---|
| **① Business-context learning** | Documents are the source of truth. Drop in a doc → the agent extracts metric/dimension/rule candidates → you confirm → they land in the semantic layer. |
| **② Two-axis robustness** | **(2a) DB robustness** — works even when columns lack descriptions (auto-enrichment, v1.5). **(2b) Semantic robustness** — teams hold *different* definitions of the same term without conflict. This axis is the product/research identity. |
| **③ Hermes memory** | Conversations, facts, and preferences persist instead of resetting each session. |
| **④ Multi-interface** | Phase 1 Discord today; Slack/Web are future adapters. No platform lock-in. |

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

### 앱에 내장하기: SQL 없는 공개 API

Discord 외의 앱은 `Lang2SQLRuntime` 공개 API를 사용할 수 있다. 흐름은
`connect → candidates → human feedback → typed plan → execute`다. 호스트와 모델은
SQL 문자열을 만들거나 받지 않으며, 사람의 검토 선택과 서버가 검증한 typed plan만
실행 경계로 넘는다. 현재 governed 실행은 **기존 파일을 read-only로 연 SQLite와
DuckDB**에 한정한다. 정확한 DTO, 검토 루프, bound `EQ`/`IN` 필터와 native `DATE`
`[start, end)` 기간창 예제는 [`docs/LIBRARY_API.md`](docs/LIBRARY_API.md)를 따른다.
DB 준비부터 결과 출력까지 한 번에 확인하려면 다음 예제를 실행한다.

```bash
uv run python examples/semantic_runtime_quickstart.py
```

### Governed semantic mode (semantic first-connect)

`/setup` now performs a PII-safe catalog scan immediately after connecting.
Physical PK/FK facts are accepted automatically, while numeric business metrics
are reviewed only when a real question needs them. In governed mode the model
cannot call `run_sql`; it selects allowlisted IDs, copies the relevant question
phrases, declares the requested aggregate and unresolved obligations, then a
deterministic compiler builds SQL. Phrase-to-column-to-aggregate bindings are
persisted after review, so SUM and AVG over one physical column do not overwrite
each other.

`/setup`이 catalog를 정상 활성화하면 해당 연결은 governed mode로 전환된다.
이 모드에서는 `run_sql`이 모델 도구 목록에서 제거되고 `semantic_query`만 서버 검증을
거쳐 SQL을 컴파일한다. catalog가 손상되더라도 legacy raw SQL 경로로 되돌아가지 않고
연결을 차단한다.

Unbounded string columns that are neither clearly sensitive nor clearly safe
stay hidden from the model until an administrator explicitly approves grouped
value disclosure with `/semantic_release`. That disclosure decision is separate
from the requester's later phrase-to-column review. Physical source-row counts
use an explicit `COUNT(*)` expression on every table, including PK-less tables.

Discord flow: `/setup` → `/semantic_status` → when needed, an administrator
uses 15-minute candidate tokens to review dimension disclosure or map an opaque
dimension/metric phrase → ask a question → independently review the metric aggregate and
dimension phrase when requested → the immutable original question resumes
without a second LLM parse. Destructive or disclosure actions use warning and
confirmation steps; metric/dimension mapping and dimension release bind the confirmation
to the same administrator and exact payload. See
[`docs/SEMANTIC_FIRST_CONNECT.md`](docs/SEMANTIC_FIRST_CONNECT.md) for the exact
supported scope and fail-closed boundaries.

Guild queries are admin-only by default. Non-admin members can query only in
parent channels explicitly listed in `LANG2SQL_DISCORD_QUERY_CHANNEL_IDS`;
threads inherit the parent channel. This frontend allowlist and the aggregate
suppression rules are not a substitute for database row/column authorization.

---

## What V1 does / does NOT do yet (honesty section)

**Does:**
- 3-scope semantic federation (guild / channel / member) with most-specific-wins
  resolution; `term_custom` registers definitions per scope (KV-backed).
- Safety pipeline with the V1 layers (whitelist + timeout), gating every query.
- Legacy raw mode includes `run_sql`, schema exploration/enrichment, semantic
  term, ingestion, memory, and clarification tools.
- Governed mode replaces the raw query/exploration surface with
  `semantic_query`, blocks sample-based enrichment, and keeps SQL in audit only.
- Memory service (in-memory store + inject-all recall + manual `/remember`).
- Discord frontend (bot, commands, session router, render).
- Encrypted-at-rest secrets (Fernet) and SQLite-backed persistence.
- PII-safe first-connect catalog, lazy metric review, and a typed semantic query
  path for aggregate/group-by queries over declared many-to-one FK paths.
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
- DB metadata auto-enrichment, AST-precise SQL validation, function blocklists,
  cost gating, `/semantic diff` / `/semantic promote`, keyword/vector recall,
  automatic fact extraction, URL/Notion ingestion — all scoped to v1.5+.
- Persist across restarts by default: the V1 `SqliteStore` defaults to in-memory;
  point it at a file for durability.
- Silently widen the typed-query boundary: advanced filters (OR/NOT, partial
  match, free search), timestamp/relative/fiscal/cohort time, unit conversion,
  derived formulas, composite joins, and fan-out joins fail closed rather than
  being guessed or dropped.
- Claim universal dialect parity: current public governed-execution evidence
  covers existing file-backed SQLite and DuckDB only. Connector availability and
  verified governed execution are reported separately; unverified remote
  dialects fail closed.

The cross-domain benchmark currently contains 28 cases over 21 public SQLite
databases: 17 cases exercise the supported typed-query scope and 11 verify
fail-closed handling of unsupported obligations. Its frozen semantic plans and
gold SQL validate compilation, disclosure and result behavior; they do not
measure natural-language planning accuracy. Dataset-specific mappings remain
under `bench/**` and are not imported by product code.

---

## Roadmap at a glance

| Area | V1 | V1.5 | V2 | V2.5 |
|---|---|---|---|---|
| **Safety** | whitelist + timeout | + AST validation, function blocklist, auto LIMIT, **metadata enrichment**, rate limit | + cost gate (EXPLAIN), per-engine pipelines | — |
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
