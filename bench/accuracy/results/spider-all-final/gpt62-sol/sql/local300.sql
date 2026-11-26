WITH RECURSIVE daily_activity AS (
    SELECT customer_id,
           DATE(txn_date) AS balance_date,
           SUM(CASE
                   WHEN txn_type = 'deposit' THEN txn_amount
                   WHEN txn_type IN ('purchase', 'withdrawal') THEN -txn_amount
               END) AS net_amount
    FROM customer_transactions
    GROUP BY customer_id, DATE(txn_date)
),
customer_dates AS (
    SELECT customer_id,
           MIN(balance_date) AS balance_date,
           MAX(balance_date) AS last_date
    FROM daily_activity
    GROUP BY customer_id
    UNION ALL
    SELECT customer_id, DATE(balance_date, '+1 day'), last_date
    FROM customer_dates
    WHERE balance_date < last_date
),
daily_balances AS (
    SELECT d.customer_id,
           d.balance_date,
           SUM(COALESCE(a.net_amount, 0)) OVER (
               PARTITION BY d.customer_id
               ORDER BY d.balance_date
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           ) AS balance
    FROM customer_dates AS d
    LEFT JOIN daily_activity AS a
        ON a.customer_id = d.customer_id
       AND a.balance_date = d.balance_date
),
customer_monthly_max AS (
    SELECT customer_id,
           STRFTIME('%Y-%m', balance_date) AS month,
           MAX(CASE WHEN balance < 0 THEN 0 ELSE balance END) AS max_daily_balance
    FROM daily_balances
    GROUP BY customer_id, STRFTIME('%Y-%m', balance_date)
)
SELECT month, SUM(max_daily_balance) AS monthly_total
FROM customer_monthly_max
GROUP BY month
ORDER BY month;
