WITH customer_daily_balances AS (
  SELECT
    customer_id,
    date(txn_date) AS transaction_date,
    SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount ELSE -txn_amount END) AS daily_net_change
  FROM customer_transactions
  GROUP BY customer_id, transaction_date
),
customer_running_balance AS (
  SELECT
    customer_id,
    transaction_date,
    SUM(daily_net_change) OVER (PARTITION BY customer_id ORDER BY transaction_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance
  FROM customer_daily_balances
),
customer_30day_rolling_avg AS (
  SELECT
    customer_id,
    transaction_date,
    MAX(0, AVG(running_balance) OVER (PARTITION BY customer_id ORDER BY transaction_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)) AS rolling_avg_balance
  FROM customer_running_balance
),
monthly_max_30day_avg AS (
  SELECT
    customer_id,
    strftime('%Y-%m', transaction_date) AS year_month,
    MAX(rolling_avg_balance) AS max_rolling_avg_balance
  FROM customer_30day_rolling_avg
  WHERE (SELECT COUNT(*) 
         FROM customer_30day_rolling_avg c2 
         WHERE c2.customer_id = customer_30day_rolling_avg.customer_id 
           AND c2.transaction_date <= customer_30day_rolling_avg.transaction_date) >= 30
  GROUP BY customer_id, year_month
),
customer_baseline_month AS (
  SELECT
    customer_id,
    MIN(strftime('%Y-%m', date(txn_date))) AS baseline_month
  FROM customer_transactions
  GROUP BY customer_id
),
final_monthly_sums AS (
  SELECT
    mm.year_month,
    SUM(mm.max_rolling_avg_balance) AS sum_max_rolling_avg_balance
  FROM monthly_max_30day_avg mm
  JOIN customer_baseline_month cb ON mm.customer_id = cb.customer_id
  WHERE mm.year_month != cb.baseline_month
  GROUP BY mm.year_month
  ORDER BY mm.year_month
)
SELECT * FROM final_monthly_sums;
