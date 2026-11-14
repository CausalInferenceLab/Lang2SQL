WITH monthly_balances AS (
  SELECT
    strftime('%Y-%m-01', date) AS month,
    user_id,
    CASE WHEN balance < 0 THEN 0 ELSE balance END AS non_negative_balance
  FROM bitcoin_transactions
),
monthly_total_balances AS (
  SELECT
    month,
    SUM(non_negative_balance) AS total_balance
  FROM monthly_balances
  GROUP BY month
),
prev_month_totals AS (
  SELECT
    month,
    LAG(total_balance) OVER (ORDER BY month) AS prev_month_total
  FROM monthly_total_balances
)
SELECT
  month,
  prev_month_total AS total_balance_prev_month
FROM prev_month_totals
WHERE prev_month_total IS NOT NULL
ORDER BY month ASC
