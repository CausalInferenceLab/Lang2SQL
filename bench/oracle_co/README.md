# Raw Codex → Lang2SQL → Oracle CO demo

Uses Oracle's official [Customer Orders sample](https://github.com/oracle-samples/db-sample-schemas/tree/main/customer_orders), tested at commit `6660bad68c07bd143430ace58565b3f727e17263` on Oracle Free 23.9. CO requires Oracle 19c or later. Its seven original tables/data are unchanged. `views.sql` adds **our demo business views**, not Oracle-provided business definitions.

## Before presenting

Install `customer_orders/co_install.sql` in a dedicated PDB. Answer **no** to overwrite: never replace an existing CO schema. The official script creates CO; install as a DBA, not as the query user. Verify 392 customers, 1,950 orders and 3,914 items. The data dates are February 2021–April 2022. Do not query 2025 or invent discount/return amount columns or currency.

Run `views.sql` once as a DBA. Create separate `L2S_CO_SALES` and `L2S_CO_FINANCE` users with CREATE SESSION and SELECT only on the seven CO tables and the three `L2S_` views. Create a separate metadata writer following `../oracle_meeting/metadata.sql`. The metadata schema stores serialized JSON in CLOB, not JSONB. Do not give the business readers metadata writes.

Prepare two private environment files outside macOS protected Documents folders if Terminal access has not been granted. Each contains `LANG2SQL_DB_URL`, `LANG2SQL_DB_SCHEMA=CO`, and `LANG2SQL_METADATA_URL`; use different business credentials. Restrict permissions to 0600 and keep files out of Git. Run `uv sync --extra oracle --extra mcp --extra tools` ahead of the event. The demo session helper requires Python 3.11+ (tomllib); the tested interpreter is 3.13.

```sh
uv run --env-file /private/sales.env python bench/oracle_co/demo.py --state /private/live.json new
uv run --env-file /private/sales.env python bench/oracle_co/demo.py --state /private/live.json prepare
uv run --env-file /private/finance.env python bench/oracle_co/demo.py --state /private/live.json --team finance prepare
```

`new` creates a unique organization and refuses to overwrite the state file. It never deletes existing database records. Rehearsal and live scopes must be different. `prepare` registers an explicit **주문 건수** definition in both teams and an explicit finance **매출** definition. Sales **매출** starts undefined. This is not an entirely empty knowledge base. Finance's illustrative rule includes refunded orders before refund; it is not a claim about accounting standards.

Use the printed organization ID in the registration below. The user's identity/team is operator configuration, not authenticated login or row-level authorization. `AGENTS.md` is necessary client behavior configuration for automatic candidate/format-preference extraction; the MCP connection alone does not guarantee extraction.

## 12-minute live sequence

1. **00:00–01:00, show the existing DB.** Run `uv run --env-file /private/sales.env python bench/oracle_co/db_preview.py`. Show real account, tables, five order rows and independent reference SQL. Say the DB/accounts and views are already prepared.
2. **01:00–02:30, register MCP and open real Codex.** Show the actual command below, then `codex mcp get lang2sql-co-sales`. Launch `python bench/oracle_co/session.py --server lang2sql-co-sales`; enter `/mcp` to show the tools. The launcher loads that actual registered server and disables other MCPs for this invocation. Tool approvals are limited to query/schema/definition-check/candidate/note tools; operator approval is not exposed.
3. **02:30–03:30, basic query.** “연결된 Oracle 테이블을 보여주고 2021년 10월 주문 건수를 조회해줘.” Expect **172** and actual SQL.
4. **03:30–04:30, undefined business concept.** “그럼 같은 달 매출은 얼마야?” Expect clarification and **no run_sql call**. If it runs SQL, stop and preserve the trace; do not call the demo successful.
5. **04:30–06:00, explain the meaning.** “우리 영업팀에서 매출의 정의는 다음 문장이야: 주문일 기준으로 COMPLETE 주문의 주문 항목 UNIT_PRICE × QUANTITY 합계. 취소와 환불 주문은 제외하며 샘플에는 통화 정보가 없어 통화 단위는 붙이지 않는다.” Show the actual candidate ID and pending status. In another terminal run `demo.py ... inspect` to show Oracle metadata.
6. **06:00–07:00, explicit operator approval.** Run `demo.py --state /private/live.json approve CANDIDATE_ID` with the sales env. It displays the candidate and view, requires exact fixture wording, and asks the human to type `APPROVE`. It calls the existing approval and binding implementations. It does not automatically approve paraphrased definitions; review those with the general operator CLI. Approval and binding are separate writes: after interruption inspect status, never blindly repeat the command.
7. **07:00–08:00, solve the same question.** “승인했어. 2021년 10월 매출을 조회해줘.” Expect **25,486.11** from CO.L2S_SALES, COMPLETE filter, October 1 inclusive/November 1 exclusive.
8. **08:00–09:00, extract another piece of context.** “나는 매출 보고를 볼 때 소수점 둘째 자리까지 표시하고 기준과 조회 기간을 먼저 보는 편이야. 그다음 달도 조회해줘.” Expect `remember` plus November **24,735.56**. Show new memory rows with `inspect`. This updates metadata; original orders are unchanged. A display preference is not a revised approved business definition.
9. **09:00–10:30, start a fresh conversation.** Exit/reopen the same sales launcher and ask “2021년 11월 매출을 조회해줘.” Show `recall_memory`, saved definition and **24,735.56** without re-explaining the rule. A fresh conversation needs an explicit month; “next month” alone lacks an anchor.
10. **10:30–12:00, switch the configured user/team.** Open the separately registered finance server and ask “2021년 10월 매출은 얼마야?” Expect **25,922.88** and finance's COMPLETE+REFUNDED definition. Explain the **436.77** difference via the definition, not model randomness.

```sh
codex mcp add lang2sql-co-sales -- uv run --project /absolute/Lang2SQL \
  --env-file /private/sales.env --no-sync lang2sql-mcp \
  --metadata-url-env LANG2SQL_METADATA_URL --org-id ORG \
  --team-id sales --user-id presenter
# Repeat with the finance env, name lang2sql-co-finance and --team-id finance.
```

Keep 10–20 seconds after each result for pointing out definition/period/SQL. Observed model turns took roughly 9–19 seconds in the initial rehearsal; this is not a guaranteed latency. Allow 30 seconds of startup slack. Personal speech speed was not measured; adjust the 12-minute budget after one spoken rehearsal. Use the remaining 8 minutes for problem, architecture and conclusion.

## Evidence and recovery

`session.py --server NAME --question TEXT --output NEW.json [--thread THREAD_ID]` records real Codex events, SQL/results, elapsed time and shell-use evidence through the existing `long_context.run_turn`. Exit code zero alone is **not** proof of success: check expected MCP tool calls and DB result. No model is pinned; record the actual selected model when comparing runs.

If MCP is unavailable, check `codex mcp get`, env-file readability from the launching app, container status and metadata connectivity. On macOS a protected Documents access dialog can stall the MCP before startup. Do not disable definition checks to recover. Resume an approved scope only after checking candidate/definition status. To start over create a fresh scope, not a destructive DB reset.

Keep original failures with corrected runs. A timed terminal-output recording can be replayed if screen capture is unavailable, but label it **terminal recording/re-render**, not a native desktop recording. Never substitute fabricated tool outputs. Native screen recording requires OS screen-recording permission. Do not record credentials or unrelated windows.

Current limits: operator CLI approval, no automatic update of a bound business definition, no Git merge/history, no real end-user authentication, and no claim of generalized accuracy improvement or automatic model training.
