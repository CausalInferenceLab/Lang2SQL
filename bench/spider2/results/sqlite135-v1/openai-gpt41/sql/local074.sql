WITH months AS (
  SELECT 
    date(strftime('%Y-%m-01', MIN(transaction_date)), '+0 month') AS month_start,
    date(strftime('%Y-%m-01', MAX(transaction_date)), '+0 month') AS month_end
  FROM customer_transactions
),
all_months AS (
  -- Generate all months between min and max transaction months
  WITH RECURSIVE month_seq(m) AS (
    SELECT month_start FROM months
    UNION ALL
    SELECT date(m, '+1 month')
    FROM month_seq, months
    WHERE date(m, '+1 month') <= month_end
  )
  SELECT m AS month FROM month_seq
),
all_combos AS (
  -- Cross join: Each customer x each month
  SELECT DISTINCT
    c.customer_id,
    m.month
  FROM (SELECT DISTINCT customer_id FROM customer_transactions) c
  CROSS JOIN all_months m
),
txn_per_month AS (
  -- Aggregated transactions per customer, month
  SELECT
    customer_id,
    strftime('%Y-%m-01', transaction_date) AS month,
    SUM(amount) AS monthly_change
  FROM customer_transactions
  GROUP BY customer_id, month
),
filled_balances AS (
  -- Join all_combos with monthly changes (may be NULL if no activity)
  SELECT
    ac.customer_id,
    ac.month,
    COALESCE(tpm.monthly_change, 0) AS monthly_change
  FROM all_combos ac
  LEFT JOIN txn_per_month tpm
    ON ac.customer_id = tpm.customer_id AND ac.month = tpm.month
),
cum_balances AS (
  -- Calculate monthly cumulative balance per customer
  SELECT
    customer_id,
    month,
    monthly_change,
    SUM(monthly_change) OVER (
      PARTITION BY customer_id
      ORDER BY month
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_balance
  FROM filled_balances
)
SELECT
  customer_id,
  month AS closing_month,
  monthly_change,
  cumulative_balance AS month_end_balance
FROM cum_balances
ORDER BY customer_id, closing_month;
