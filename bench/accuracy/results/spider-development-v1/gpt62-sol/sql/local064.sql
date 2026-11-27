WITH RECURSIVE months(month_start) AS (
    VALUES ('2020-01-01')
    UNION ALL
    SELECT date(month_start, '+1 month')
    FROM months
    WHERE month_start < '2020-12-01'
),
customers AS (
    SELECT customer_id FROM customer_nodes
    UNION
    SELECT customer_id FROM customer_transactions
),
monthly_transactions AS (
    SELECT customer_id,
           date(txn_date, 'start of month') AS month_start,
           SUM(CASE txn_type
                   WHEN 'deposit' THEN txn_amount
                   WHEN 'withdrawal' THEN -txn_amount
               END) AS balance
    FROM customer_transactions
    WHERE txn_date >= '2020-01-01'
      AND txn_date < '2021-01-01'
      AND txn_type IN ('deposit', 'withdrawal')
    GROUP BY customer_id, date(txn_date, 'start of month')
),
monthly_stats AS (
    SELECT m.month_start,
           COUNT(CASE WHEN t.balance > 0 THEN 1 END) AS positive_customers,
           AVG(COALESCE(t.balance, 0)) AS avg_balance
    FROM months AS m
    CROSS JOIN customers AS c
    LEFT JOIN monthly_transactions AS t
      ON t.customer_id = c.customer_id
     AND t.month_start = m.month_start
    GROUP BY m.month_start
),
highest AS (
    SELECT *
    FROM monthly_stats
    ORDER BY positive_customers DESC, month_start
    LIMIT 1
),
lowest AS (
    SELECT *
    FROM monthly_stats
    ORDER BY positive_customers ASC, month_start
    LIMIT 1
)
SELECT strftime('%Y-%m', h.month_start) AS highest_month,
       h.positive_customers AS highest_positive_customer_count,
       h.avg_balance AS highest_month_avg_balance,
       strftime('%Y-%m', l.month_start) AS lowest_month,
       l.positive_customers AS lowest_positive_customer_count,
       l.avg_balance AS lowest_month_avg_balance,
       h.avg_balance - l.avg_balance AS average_balance_difference
FROM highest AS h
CROSS JOIN lowest AS l;
