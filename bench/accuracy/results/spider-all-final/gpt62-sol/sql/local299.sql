WITH RECURSIVE
  daily_transactions AS (
    SELECT customer_id,
           DATE(txn_date) AS transaction_date,
           SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount
                    ELSE -txn_amount END) AS net_amount
    FROM customer_transactions
    GROUP BY customer_id, DATE(txn_date)
  ),
  reporting_end AS (
    SELECT MAX(transaction_date) AS last_date
    FROM daily_transactions
  ),
  customer_days(customer_id, balance_date, baseline_month) AS (
    SELECT customer_id,
           MIN(transaction_date),
           STRFTIME('%Y-%m', MIN(transaction_date))
    FROM daily_transactions
    GROUP BY customer_id

    UNION ALL

    SELECT d.customer_id, DATE(d.balance_date, '+1 day'), d.baseline_month
    FROM customer_days AS d
    CROSS JOIN reporting_end AS e
    WHERE d.balance_date < e.last_date
  ),
  daily_balances AS (
    SELECT d.customer_id, d.balance_date, d.baseline_month,
           SUM(COALESCE(t.net_amount, 0)) OVER (
             PARTITION BY d.customer_id
             ORDER BY d.balance_date
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           ) AS daily_balance
    FROM customer_days AS d
    LEFT JOIN daily_transactions AS t
      ON t.customer_id = d.customer_id
     AND t.transaction_date = d.balance_date
  ),
  rolling_balances AS (
    SELECT customer_id, STRFTIME('%Y-%m', balance_date) AS month,
           baseline_month,
           AVG(daily_balance) OVER trailing_30_days AS average_balance,
           COUNT(*) OVER trailing_30_days AS days_in_window
    FROM daily_balances
    WINDOW trailing_30_days AS (
      PARTITION BY customer_id
      ORDER BY balance_date
      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    )
  ),
  customer_monthly_max AS (
    SELECT customer_id, month,
           MAX(CASE WHEN average_balance < 0 THEN 0.0
                    ELSE average_balance END) AS max_30_day_average
    FROM rolling_balances
    WHERE days_in_window = 30 AND month > baseline_month
    GROUP BY customer_id, month
  )
SELECT month, SUM(max_30_day_average) AS total_max_30_day_average_balance
FROM customer_monthly_max
GROUP BY month
ORDER BY month;
