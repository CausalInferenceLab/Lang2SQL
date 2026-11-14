WITH monthly_net AS (
  SELECT
    member_id,
    DATE(strftime('%Y-%m-01', txn_date)) AS month,
    SUM(CASE WHEN txn_type = 'deposit' THEN quantity ELSE -quantity END) AS net_amount
  FROM bitcoin_transactions
  GROUP BY member_id, month
),
monthly_balance AS (
  SELECT
    member_id,
    month,
    SUM(net_amount) OVER (PARTITION BY member_id ORDER BY month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS closing_balance
  FROM monthly_net
),
growth_rates AS (
  SELECT
    a.member_id,
    a.month,
    a.closing_balance,
    b.closing_balance AS prev_closing_balance,
    CASE 
      WHEN b.closing_balance = 0 OR b.closing_balance IS NULL THEN a.closing_balance * 100
      ELSE (a.closing_balance - b.closing_balance) / b.closing_balance * 100
    END AS growth_rate
  FROM monthly_balance a
  LEFT JOIN monthly_balance b ON a.member_id = b.member_id AND a.month = DATE(b.month, '+1 month')
),
latest_month AS (
  SELECT
    member_id,
    max(month) AS last_month
  FROM growth_rates
  GROUP BY member_id
),
latest_growth AS (
  SELECT gr.member_id, gr.growth_rate
  FROM growth_rates gr
  JOIN latest_month lm ON gr.member_id = lm.member_id AND gr.month = lm.last_month
)

SELECT
  100.0 * SUM(CASE WHEN growth_rate > 5 THEN 1 ELSE 0 END) / COUNT(*) AS percentage_customers_growth_above_5
FROM latest_growth;
