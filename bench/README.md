# bench/ — runnable demo

A self-contained walkthrough of Lang2SQL V1's headline value, with **no Discord
token and no live database**. It runs the real shipped modules against the
canned Postgres stub and authenticated Codex CLI.

## Run

```bash
.venv/bin/python bench/ecommerce_demo.py
```

Install Codex CLI and run `codex login`. Section 0 uses real Codex inference;
other sections exercise local federation and safety code.

## What each section proves

| Section | What it shows | Maps to |
|---|---|---|
| **0 — assembled harness** | `ContextConcierge` wires CodexLLM + canned `PostgresExplorer` + nine tools into a `HarnessContext`, and `agent_loop` drives one full LLM→tool→LLM cycle. A *wiring* proof, not an intelligence proof. | harness + concierge |
| **1 — define metrics** | Three e-commerce metrics (`total_revenue`, `aov`, `paid_orders`) are written to the current channel's scope and read back from the effective layer. | ★① context learning |
| **2 — federation (the money shot)** | `active_user` is defined as *"30-day login"* in `#marketing` and *"paid subscriber"* in `#finance`. Each channel resolves its **own** definition by walking its scope chain — same term, two live meanings, **zero conflict**. | **★④ semantic federation** |
| **3 — safety gate** | `DROP`, `INSERT`, and a CTE-hidden `INSERT` are all **blocked fail-closed**; a plain `SELECT ... GROUP BY` **passes**. The read-only guarantee is enforced before any SQL would reach a database. | **★① safety pipeline** |

The two headline pillars demonstrated are **★① safety** (Sections 0 + 3) and
**★④ federation** (Sections 1 + 2).

## Honesty notes

- The `PostgresExplorer` is a **V1 stub**: it returns canned `orders`/`users`
  schema and sample rows and does not connect to a real database. Real psycopg
  execution lands in v1.5.
- Codex performs inference while the database remains a canned fixture. Tests explicitly inject FakeLLM at the composition seam to run offline.
- Federation state in Sections 1–2 lives in an in-memory `SemanticStore`; the
  SQLite-backed persistent store is wired through the concierge for the bot.

## Oracle 데모

실제 Oracle의 부서별 매출·되묻기·권한 차단 시연: [재현 절차](../docs/oracle_demo.md).
