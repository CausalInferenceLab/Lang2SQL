WITH RECURSIVE
daily_changes AS (
    SELECT customer_id,
           txn_date AS balance_date,
           SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount ELSE -txn_amount END) AS net_change
    FROM main.customer_transactions
    GROUP BY customer_id, txn_date
),
customer_starts AS (
    SELECT customer_id, MIN(balance_date) AS first_day
    FROM daily_changes
    GROUP BY customer_id
),
reporting_end AS (
    SELECT MAX(balance_date) AS last_day
    FROM daily_changes
),
customer_days(customer_id, balance_date, baseline_month) AS (
    SELECT customer_id, first_day, strftime('%Y-%m', first_day)
    FROM customer_starts

    UNION ALL

    SELECT customer_id, date(balance_date, '+1 day'), baseline_month
    FROM customer_days
    WHERE balance_date < (SELECT last_day FROM reporting_end)
),
daily_balances AS (
    SELECT c.customer_id,
           c.balance_date,
           c.baseline_month,
           SUM(COALESCE(d.net_change, 0)) OVER (
               PARTITION BY c.customer_id
               ORDER BY c.balance_date
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           ) AS running_balance
    FROM customer_days AS c
    LEFT JOIN daily_changes AS d
        ON d.customer_id = c.customer_id
       AND d.balance_date = c.balance_date
),
rolling_balances AS (
    SELECT customer_id,
           strftime('%Y-%m', balance_date) AS month,
           baseline_month,
           COUNT(*) OVER trailing_30_days AS days_in_window,
           AVG(running_balance) OVER trailing_30_days AS average_balance
    FROM daily_balances
    WINDOW trailing_30_days AS (
        PARTITION BY customer_id
        ORDER BY balance_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    )
),
customer_monthly_maxima AS (
    SELECT customer_id,
           month,
           MAX(MAX(average_balance, 0.0)) AS max_30_day_average_balance
    FROM rolling_balances
    WHERE days_in_window = 30
      AND month > baseline_month
    GROUP BY customer_id, month
)
SELECT month,
       SUM(max_30_day_average_balance) AS total_max_30_day_average_balance
FROM customer_monthly_maxima
GROUP BY month
ORDER BY month
LIMIT 1000
