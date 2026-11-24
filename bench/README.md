# bench/ — runnable demo

A self-contained walkthrough of Lang2SQL V1's headline value, with **no Discord
token and no live database**. It runs the real shipped modules against the
canned Postgres stub and the offline `FakeLLM`.

## Run

```bash
.venv/bin/python bench/ecommerce_demo.py
```

No environment variables are needed. (If `OPENAI_API_KEY` happens to be set,
Section 0 will route through OpenAI instead of the FakeLLM — everything else is
offline.)

## What each section proves

| Section | What it shows | Maps to |
|---|---|---|
| **0 — assembled harness** | `ContextConcierge` wires the FakeLLM + canned `PostgresExplorer` + six tools into a `HarnessContext`, and `agent_loop` drives one full LLM→tool→LLM cycle. A *wiring* proof, not an intelligence proof. | harness + concierge |
| **1 — define metrics** | Three e-commerce metrics (`total_revenue`, `aov`, `paid_orders`) are written to the current channel's scope and read back from the effective layer. | ★① context learning |
| **2 — federation (the money shot)** | `active_user` is defined as *"30-day login"* in `#marketing` and *"paid subscriber"* in `#finance`. Each channel resolves its **own** definition by walking its scope chain — same term, two live meanings, **zero conflict**. | **★④ semantic federation** |
| **3 — safety gate** | `DROP`, `INSERT`, and a CTE-hidden `INSERT` are all **blocked fail-closed**; a plain `SELECT ... GROUP BY` **passes**. The read-only guarantee is enforced before any SQL would reach a database. | **★① safety pipeline** |

The two headline pillars demonstrated are **★① safety** (Sections 0 + 3) and
**★④ federation** (Sections 1 + 2).

## Honesty notes

- The `PostgresExplorer` is a **V1 stub**: it returns canned `orders`/`users`
  schema and sample rows and does not connect to a real database. Real psycopg
  execution lands in v1.5.
- The `FakeLLM` is a deterministic stub for offline runs; it does not reason. It
  blindly calls the first available tool, which is why Section 0's turn ends at
  the safety gate rather than counting orders. With a real key it would call
  `gpt-4.1-mini`.
- Federation state in Sections 1–2 lives in an in-memory `SemanticStore`; the
  SQLite-backed persistent store is wired through the concierge for the bot.

## Oracle 데모

실제 Oracle의 부서별 매출·되묻기·권한 차단 시연: [재현 절차](../docs/oracle_demo.md).
