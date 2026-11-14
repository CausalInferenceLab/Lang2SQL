# Deploying the Lang2SQL Discord bot

This guide covers running the **Phase 1 Discord frontend** (`lang2sql-bot`). Be
honest with yourself about scope first: see [§What's stub](#whats-stub-be-honest).

---

## 1. Codex runtime and environment variables

Install Codex CLI on the host and run `codex login` as the same OS user that runs the bot. Ensure `codex` is on the service PATH and its authentication home is available to that user. CLI, query, ingestion, schema enrichment and organization setup all use Codex. No OpenAI/local/FakeLLM backend is selected automatically. Each inference step starts an isolated `codex exec` process; account usage limits apply.

| Variable | Required | Purpose |
|---|---|---|
| `DISCORD_BOT_TOKEN` | **yes** | Bot token from the Discord developer portal. The bot raises a clear error and exits if this is unset. |
| `LANG2SQL_LLM_MODEL` | no | Codex model ID; unset uses CLI default. |
| `LANG2SQL_LLM_TIMEOUT` | no | Per-call deadline, default 180 seconds. |
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
   reads message text to answer @mentions and thread replies).
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

The bot connects to the gateway and serves. Mention it in a channel or DM it.

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
- Set `DISCORD_BOT_TOKEN`, `LANG2SQL_SECRET_KEY` with
  `fly secrets set`. Provision Codex CLI and authentication for the service user separately.

A minimal `systemd` unit:

```ini
[Service]
WorkingDirectory=/opt/lang2sql
ExecStart=/opt/lang2sql/.venv/bin/lang2sql-bot
Environment=DISCORD_BOT_TOKEN=...
Environment=LANG2SQL_LLM_TIMEOUT=180
# Ensure the service user can find codex and has completed codex login.
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

- Without a configured DB, the default explorer is a canned stub. Configure a real DB URL or guild DSN and install its driver extra for real execution.
- Codex CLI must be installed and authenticated as the service account. There is no automatic fake/provider fallback when authentication fails.
- **No rate limiting** in V1 — keep deployments to small trial guilds so token
  spend stays bounded (rate limit + per-user token caps are v1.5).
