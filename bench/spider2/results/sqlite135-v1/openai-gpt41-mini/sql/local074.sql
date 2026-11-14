WITH month_series AS (
  SELECT date(substr(min(txn_date), 1, 7) || '-01') AS month_start
  FROM customer_transactions
  UNION ALL
  SELECT date(month_start, '+1 month')
  FROM month_series
  WHERE month_start < date('now', 'start of month')
),
customer_months AS (
  SELECT DISTINCT customer_id, month_start
  FROM customer_transactions
  CROSS JOIN month_series 
  WHERE month_start <= date('now', 'start of month')
),
monthly_balances AS (
  SELECT
    cm.customer_id,
    cm.month_start,
    COALESCE(
      (SELECT sum(txn_amount)
       FROM customer_transactions ct
       WHERE ct.customer_id = cm.customer_id
         AND date(ct.txn_date) <= date(cm.month_start, '+1 month', '-1 day')
      ),
      0
    ) AS closing_balance
  FROM customer_months cm
),
monthly_changes AS (
  SELECT
    customer_id,
    month_start,
    closing_balance,
    closing_balance - LAG(closing_balance, 1, 0) OVER (PARTITION BY customer_id ORDER BY month_start) AS monthly_change
  FROM monthly_balances
),
cumulative_balances AS (
  SELECT 
    customer_id,
    month_start,
    closing_balance,
    monthly_change,
    SUM(monthly_change) OVER (PARTITION BY customer_id ORDER BY month_start ROWS UNBOUNDED PRECEDING) AS cumulative_balance
  FROM monthly_changes
)
SELECT * FROM cumulative_balances
ORDER BY customer_id, month_start;
