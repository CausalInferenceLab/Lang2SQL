WITH gold_accounts AS (
    SELECT DISTINCT d.account_id
    FROM card AS c
    JOIN disp AS d ON d.disp_id = c.disp_id
    WHERE c.type = 'gold'
), transaction_totals AS (
    SELECT t.account_id,
           SUM(CASE WHEN t.type = 'PRIJEM' THEN t.amount ELSE 0 END) AS total_received,
           SUM(CASE WHEN t.type = 'VYDAJ' THEN t.amount ELSE 0 END) AS total_withdrawn
    FROM trans AS t
    JOIN gold_accounts AS g ON g.account_id = t.account_id
    GROUP BY t.account_id
), loan_counts AS (
    SELECT l.account_id, COUNT(*) AS loan_count
    FROM loan AS l
    JOIN gold_accounts AS g ON g.account_id = l.account_id
    GROUP BY l.account_id
), external_counterparties AS (
    SELECT DISTINCT t.account_id, t.bank, t."account" AS counterparty_account
    FROM trans AS t
    JOIN gold_accounts AS g ON g.account_id = t.account_id
    WHERE t.operation IN ('PREVOD NA UCET', 'PREVOD Z UCTU')
      AND NULLIF(TRIM(t.bank), '') IS NOT NULL
      AND t."account" IS NOT NULL
), counterparty_counts AS (
    SELECT account_id, COUNT(*) AS external_counterparty_count
    FROM external_counterparties
    GROUP BY account_id
)
SELECT g.account_id,
       di.A2 AS district_name,
       di.A3 AS region,
       COALESCE(tt.total_received, 0) AS total_received,
       COALESCE(tt.total_withdrawn, 0) AS total_withdrawn,
       COALESCE(lc.loan_count, 0) AS loan_count,
       COALESCE(cc.external_counterparty_count, 0) AS external_counterparty_count,
       DENSE_RANK() OVER (
           ORDER BY COALESCE(cc.external_counterparty_count, 0) DESC
       ) AS counterparty_rank
FROM gold_accounts AS g
JOIN account AS a ON a.account_id = g.account_id
JOIN district AS di ON di.district_id = a.district_id
LEFT JOIN transaction_totals AS tt ON tt.account_id = g.account_id
LEFT JOIN loan_counts AS lc ON lc.account_id = g.account_id
LEFT JOIN counterparty_counts AS cc ON cc.account_id = g.account_id
ORDER BY counterparty_rank, g.account_id;
