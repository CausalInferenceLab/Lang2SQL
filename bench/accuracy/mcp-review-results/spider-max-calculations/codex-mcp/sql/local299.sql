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
    SELECT days.customer_id, days.balance_date, days.baseline_month,
           SUM(COALESCE(changes.net_change, 0)) OVER (
               PARTITION BY days.customer_id
               ORDER BY days.balance_date
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           ) AS closing_balance
    FROM customer_days AS days
    LEFT JOIN daily_changes AS changes
        ON changes.customer_id = days.customer_id
       AND changes.txn_date = days.balance_date
),
rolling_averages AS (
    SELECT customer_id, strftime('%Y-%m', balance_date) AS month, baseline_month,
           AVG(closing_balance) OVER trailing_30_days AS avg_balance,
           COUNT(*) OVER trailing_30_days AS days_in_window
    FROM daily_balances
    WINDOW trailing_30_days AS (
        PARTITION BY customer_id
        ORDER BY balance_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    )
),
customer_monthly_maxima AS (
    SELECT customer_id, month,
           MAX(CASE WHEN avg_balance < 0 THEN 0.0 ELSE avg_balance END) AS max_avg_balance
    FROM rolling_averages
    WHERE days_in_window = 30 AND month > baseline_month
    GROUP BY customer_id, month
)
SELECT month, SUM(max_avg_balance) AS total_max_30_day_avg_balance
FROM customer_monthly_maxima
GROUP BY month
ORDER BY month
LIMIT 1000
