WITH RECURSIVE
daily_changes AS (
    SELECT customer_id, txn_date,
           SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount ELSE -txn_amount END) AS net_change
    FROM main.customer_transactions
    GROUP BY customer_id, txn_date
),
customer_days(customer_id, balance_date, baseline_month) AS (
    SELECT customer_id, MIN(txn_date), strftime('%Y-%m', MIN(txn_date))
    FROM daily_changes
    GROUP BY customer_id

    UNION ALL

    SELECT customer_id, date(balance_date, '+1 day'), baseline_month
    FROM customer_days
    WHERE balance_date < (SELECT MAX(txn_date) FROM daily_changes)
),
daily_balances AS (
    SELECT d.customer_id, d.balance_date, d.baseline_month,
           SUM(COALESCE(c.net_change, 0)) OVER (
               PARTITION BY d.customer_id ORDER BY d.balance_date
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           ) AS closing_balance
    FROM customer_days AS d
    LEFT JOIN daily_changes AS c
      ON c.customer_id = d.customer_id AND c.txn_date = d.balance_date
),
rolling_balances AS (
    SELECT customer_id, strftime('%Y-%m', balance_date) AS month, baseline_month,
           COUNT(*) OVER trailing_days AS day_count,
           AVG(closing_balance) OVER trailing_days AS avg_balance
    FROM daily_balances
    WINDOW trailing_days AS (
        PARTITION BY customer_id ORDER BY balance_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    )
),
customer_monthly_maxima AS (
    SELECT customer_id, month,
           MAX(CASE WHEN avg_balance < 0 THEN 0.0 ELSE avg_balance END) AS max_avg_balance
    FROM rolling_balances
    WHERE day_count = 30 AND month > baseline_month
    GROUP BY customer_id, month
)
SELECT month, SUM(max_avg_balance) AS total_max_30_day_avg_balance
FROM customer_monthly_maxima
GROUP BY month
ORDER BY month
LIMIT 1000
