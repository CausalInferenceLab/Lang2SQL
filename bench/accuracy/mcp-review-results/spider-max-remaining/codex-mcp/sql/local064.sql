WITH RECURSIVE months(month_start) AS (
    VALUES ('2020-01-01')
    UNION ALL
    SELECT date(month_start, '+1 month')
    FROM months
    WHERE month_start < '2020-12-01'
),
customers AS (
    SELECT DISTINCT customer_id
    FROM main.customer_nodes
),
balances AS (
    SELECT
        customer_id,
        date(txn_date, 'start of month') AS month_start,
        SUM(CASE txn_type
                WHEN 'deposit' THEN txn_amount
                WHEN 'withdrawal' THEN -txn_amount
                ELSE 0
            END) AS balance
    FROM main.customer_transactions
    WHERE txn_date >= '2020-01-01'
      AND txn_date < '2021-01-01'
    GROUP BY customer_id, month_start
),
monthly AS (
    SELECT
        m.month_start,
        COUNT(*) FILTER (WHERE b.balance > 0) AS positive_customers,
        AVG(COALESCE(b.balance, 0)) AS average_balance
    FROM months AS m
    CROSS JOIN customers AS c
    LEFT JOIN balances AS b
      ON b.customer_id = c.customer_id
     AND b.month_start = m.month_start
    GROUP BY m.month_start
)
SELECT
    strftime('%Y-%m', hi.month_start) AS highest_count_month,
    hi.average_balance AS highest_count_month_average,
    strftime('%Y-%m', lo.month_start) AS lowest_count_month,
    lo.average_balance AS lowest_count_month_average,
    hi.average_balance - lo.average_balance AS average_balance_difference
FROM monthly AS hi
CROSS JOIN monthly AS lo
WHERE hi.positive_customers = (SELECT MAX(positive_customers) FROM monthly)
  AND lo.positive_customers = (SELECT MIN(positive_customers) FROM monthly)
ORDER BY hi.month_start, lo.month_start
LIMIT 1000
