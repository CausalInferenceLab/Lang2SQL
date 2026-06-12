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

> **An open-source data agent that turns natural language into SQL.**
> Not on a clean, well-documented database — on the **messy real world**, where
> columns have no descriptions and every team means something different by the
> same word.

📄 한국어: [`README.ko.md`](README.ko.md) · 🧭 Full picture: [`docs/PROJECT.md`](docs/PROJECT.md) · 🏗️ Architecture: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)

---

## In one minute

Ask the bot a question in Discord → it writes SQL, runs it, and answers.
What's different from other text-to-SQL tools isn't "question → SQL" itself —
it's everything *around* it:

- **🧩 Fill empty metadata (enrich)** — even with no column descriptions, the
  agent reads the *actual values* to infer what each column means and how tables
  join, and writes that into the semantic layer.
- **🗂️ Per-team definitions (federation)** — the same "active customer" can mean
  different things to Marketing and Finance, with no conflict. A company-wide
  default sits underneath, and the **closest definition wins (member > team > company)**.
- **🛡️ Safety** — every query is checked before it runs; only reads (SELECT) are allowed.

> Discord is the Phase 1 interface, not the identity. Slack/Web are adapters on the same core.

---

## Quickstart 1 — offline demo (no token, no database)

The fastest way to see the core. No Discord token, no real DB.

```bash
uv sync                                   # create .venv + install deps
.venv/bin/python bench/ecommerce_demo.py  # federation + safety demo
```

Shows one term resolving to two team definitions with zero conflict, and the
safety gate (DROP/INSERT blocked, SELECT passes). See [`bench/README.md`](bench/README.md).

## Quickstart 2 — CLI (for developers)

```bash
.venv/bin/lang2sql "list the tables"
```

With `OPENAI_API_KEY` set it uses `gpt-4.1-mini`; otherwise the offline `FakeLLM`
(canned behavior, no real reasoning).

---

## Discord bot setup (step by step)

### 0. Prerequisites
- Python **3.10+**, [uv](https://docs.astral.sh/uv/)
- A Discord account and a server to invite the bot to

### 1. Install
```bash
git clone https://github.com/CausalInferenceLab/lang2sql.git
cd lang2sql
uv sync
```

### 2. Create the Discord bot
1. [Discord Developer Portal](https://discord.com/developers/applications) → **New Application**
2. **Bot** tab → **Reset Token** → copy it (this is `DISCORD_BOT_TOKEN`)
3. Same screen → enable **Privileged Gateway Intents → MESSAGE CONTENT INTENT** (needed to read mentions)
4. **OAuth2 → URL Generator** → scopes `bot` + `applications.commands` → pick permissions → open the generated URL to **invite the bot** to your test server

### 3. Configure environment
```bash
cp .env.example .env
```
```ini
DISCORD_BOT_TOKEN=your_bot_token       # required
OPENAI_API_KEY=sk-...                  # for real answers (else FakeLLM)
LANG2SQL_SECRET_KEY=                   # optional — Fernet key to encrypt secrets
LANG2SQL_DATA_PATH=lang2sql_data.db    # optional — persistence file
LANG2SQL_SYNC_COMMANDS=true            # register slash commands (/setup, ...)
LANG2SQL_DB_URL=                       # optional — a default DB for all channels
```
Generate a Fernet key if you want one:
```bash
.venv/bin/python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```
> ⚠️ The app does not auto-load `.env`. Load it into your shell first:
> `set -a; source .env; set +a`

### 4. Run the bot
```bash
set -a; source .env; set +a   # load .env into the environment
.venv/bin/lang2sql-bot
```
It exits with a clear error if `DISCORD_BOT_TOKEN` is unset; otherwise it connects
to the gateway and serves. Full hosting guide: [`docs/DEPLOY.md`](docs/DEPLOY.md).

### 5. Connect a database
Two ways:

**(A) `/setup` in Discord** — a guided form for non-developers (no DSN typing).
Pick the DB type, fill the form; it tests the connection and stores credentials
encrypted. Supports **PostgreSQL · MySQL · BigQuery · Snowflake · DuckDB · Cloudflare D1**.
(DuckDB: put `/absolute/path/file.duckdb` in the path field.)

**(B) `LANG2SQL_DB_URL`** — set before launch to point every channel at one DB:
```ini
LANG2SQL_DB_URL=postgresql://user:pw@host:5432/db
LANG2SQL_DB_URL=duckdb:////absolute/path/file.duckdb   # 4 slashes = absolute path
```
> `/connect` is a V1 stub (stores the string but does not actually connect) — use `/setup`.

### 6. Use it
- **Ask in natural language** — mention the bot or DM it: `@Lang2SQL revenue by country`
- **`/enrich`** — auto-fill column meanings & relationships (big quality boost)
- **`/term_custom`, `/org_setup`** — define team-specific business terms

---

## Slash commands

| Command | What it does |
|---|---|
| `/setup` | Connect a DB via a guided form (no DSN) — **the real connection path** |
| `/enrich` | Auto-enrich column metadata (`clear:true` resets) |
| `/term_custom` | Register / show (`action:show`) / remove (`action:remove`) business terms |
| `/org_setup` | Register org (`org:`) / team (`team:`) + auto-extract terms by scanning the DB |
| `/remember` | Remember a fact for later |
| `/ingest` | Propose definitions from a document |
| `/audit_me` | Show your recent activity |
| `/connect` | (V1 stub — stores only, don't use) |

Natural-language questions go through **mentions/DM**, not slash commands — the
agent calls the tools above itself when needed.

---

## What works / what's next (honest)

**Works**
- 3-layer federation (company / team / personal), closest-definition-wins, plus
  registering definitions through conversation.
- Real external DB connections (PostgreSQL / MySQL / DuckDB / BigQuery / Snowflake / D1, via SQLAlchemy).
- `enrich` — infers column meanings & relationships from sampled real values.
- Safety pipeline (read-only, blocks risky SQL), eight tools, encrypted secrets, SQLite persistence.

**Not yet**
- Large-scale validation on a real production DB (tracked via our dirty-data benchmark).
- Deeper auto-enrichment, vector recall, URL/Notion ingestion, cost gating — scoped to v1.5+.

See [`docs/discord_first_redesign_v4_1.md`](docs/discord_first_redesign_v4_1.md) for the full architecture write-up.

---

## 🤝 기여하기

**처음 보시는 분은 [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)** — 디렉토리·레이어 책임, 한 메시지의 lifecycle, *어디를 수정하면 좋을지* 가 한곳에 정리돼 있습니다.

```bash
git clone https://github.com/CausalInferenceLab/lang2sql.git
cd lang2sql
uv sync
.venv/bin/pytest -q          # 12 safety regressions + full suite must pass
```

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
