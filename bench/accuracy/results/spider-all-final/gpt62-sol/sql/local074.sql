WITH RECURSIVE
month_bounds AS (
    SELECT
        date(MIN(txn_date), 'start of month') AS first_month,
        date(MAX(txn_date), 'start of month') AS last_month
    FROM customer_transactions
),
months(month_start) AS (
    SELECT first_month
    FROM month_bounds
    WHERE first_month IS NOT NULL

    UNION ALL

    SELECT date(m.month_start, '+1 month')
    FROM months AS m
    CROSS JOIN month_bounds AS b
    WHERE m.month_start < b.last_month
),
customers AS (
    SELECT DISTINCT customer_id
    FROM customer_transactions
),
monthly_transactions AS (
    SELECT
        customer_id,
        date(txn_date, 'start of month') AS month_start,
        SUM(CASE txn_type
            WHEN 'deposit' THEN txn_amount
            WHEN 'withdrawal' THEN -txn_amount
            WHEN 'purchase' THEN -txn_amount
        END) AS monthly_change
    FROM customer_transactions
    GROUP BY customer_id, date(txn_date, 'start of month')
),
customer_months AS (
    SELECT
        c.customer_id,
        m.month_start,
        COALESCE(t.monthly_change, 0) AS monthly_change
    FROM customers AS c
    CROSS JOIN months AS m
    LEFT JOIN monthly_transactions AS t
        ON t.customer_id = c.customer_id
       AND t.month_start = m.month_start
)
SELECT
    customer_id,
    date(month_start, '+1 month', '-1 day') AS month_end,
    monthly_change,
    SUM(monthly_change) OVER (
        PARTITION BY customer_id
        ORDER BY month_start
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS closing_balance
FROM customer_months
ORDER BY customer_id, month_start;
