# Deploying the Lang2SQL Discord bot

This guide covers running the **Phase 1 Discord frontend** (`lang2sql-bot`). Be
honest with yourself about scope first: see [§What's stub](#whats-stub-be-honest).

---

## 1. Environment variables

| Variable | Required | Purpose |
|---|---|---|
| `DISCORD_BOT_TOKEN` | **yes** | Bot token from the Discord developer portal. The bot raises a clear error and exits if this is unset. |
| `OPENAI_API_KEY` | for real use | Uses OpenAI when set. Alternatively set `LANG2SQL_LLM_BASE_URL` and `LANG2SQL_LLM_MODEL` for an OpenAI-compatible local model. With neither, `FakeLLM` is installation-smoke only. |
| `LANG2SQL_SECRET_KEY` | no | A urlsafe-base64 Fernet key used to encrypt stored secrets (DSNs/API keys) at rest. If unset, a key is auto-generated and persisted in the SQLite kv table — self-contained but only as private as the DB file. **Set this in production** so secrets decrypt across restarts and machines. |

Generate a Fernet key:

```bash
.venv/bin/python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Copy [`.env.example`](../.env.example) to `.env` and fill it in. (The bot reads
from the process environment; use your hosting platform's secrets mechanism or a
tool like `direnv`/`dotenv` to export them.)

---

## 2. Create the Discord application and bot

1. Go to the [Discord Developer Portal](https://discord.com/developers/applications) → **New Application**.
2. **Bot** tab → **Add Bot** → **Reset Token** → copy it into `DISCORD_BOT_TOKEN`.
3. **Privileged Gateway Intents** → enable **MESSAGE CONTENT INTENT** (the bot
   reads message text to answer explicit @mentions).
4. **OAuth2 → URL Generator**:
   - Scopes: `bot`
   - Bot permissions: **Send Messages**, **Read Message History**, **Attach
     Files** (for CSV results), **Create Public Threads**, **Send Messages in
     Threads**.
5. Open the generated invite URL and add the bot to your test guild.

Then run it:

```bash
export DISCORD_BOT_TOKEN=...
.venv/bin/lang2sql-bot
```

The bot connects to the gateway and serves. Explicitly mention the bot in a
channel, thread, or DM; plain messages and `@everyone`/`@here` are ignored.

---

## 3. Hosting options (free tiers)

Per the v4.1 plan (§4.1), V1 targets a free always-on host:

### Oracle Cloud Always Free
- Provision an **Always Free** ARM (Ampere A1) or AMD micro VM.
- Install uv, clone the repo, `uv sync`.
- Export the env vars and run `lang2sql-bot` under a process supervisor
  (`systemd` unit or `tmux`/`screen` for a quick trial).

### fly.io (free allowance)
- A tiny `fly.toml` running `lang2sql-bot` as the process; the bot is a
  long-lived gateway client, not an HTTP server, so no exposed ports are needed.
- Set `DISCORD_BOT_TOKEN`, `OPENAI_API_KEY`, `LANG2SQL_SECRET_KEY` with
  `fly secrets set`.

A minimal `systemd` unit:

```ini
[Service]
WorkingDirectory=/opt/lang2sql
ExecStart=/opt/lang2sql/.venv/bin/lang2sql-bot
Environment=DISCORD_BOT_TOKEN=...
Environment=OPENAI_API_KEY=...
Environment=LANG2SQL_SECRET_KEY=...
Restart=on-failure
```

---

## 4. Persistence

The V1 `SqliteStore` **defaults to `:memory:`**, so audit/session/secret state is
lost on restart. For a real deployment, construct the store with a file path so it
survives restarts and back it up alongside `LANG2SQL_SECRET_KEY` (you need both to
decrypt stored secrets).

---

## What's stub (be honest)

- **The default no-DB demo is stubbed.** Without `LANG2SQL_DB_URL` or a completed
  Discord `/setup`, the concierge uses the canned `PostgresExplorer` fixture.
  `/setup` connections use the real SQLAlchemy/D1 adapters; the required driver
  and network access must be present for that database.
- **No reasoning without a configured model.** The offline `FakeLLM` produces
  deterministic tool cycles, not real answers. Configure `OPENAI_API_KEY`, or
  both `LANG2SQL_LLM_BASE_URL` and `LANG2SQL_LLM_MODEL` for a local compatible
  server such as Ollama.
- **No rate limiting** in V1 — keep deployments to small trial guilds so token
  spend stays bounded (rate limit + per-user token caps are v1.5).
