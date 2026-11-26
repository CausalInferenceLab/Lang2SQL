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
monthly_net AS (
    SELECT customer_id,
           date(txn_date, 'start of month') AS month_start,
           SUM(CASE txn_type
                   WHEN 'deposit' THEN txn_amount
                   WHEN 'withdrawal' THEN -txn_amount
                   ELSE 0
               END) AS balance
    FROM customer_transactions
    WHERE txn_date >= '2020-01-01' AND txn_date < '2021-01-01'
    GROUP BY customer_id, date(txn_date, 'start of month')
),
month_stats AS (
    SELECT m.month_start,
           COUNT(CASE WHEN n.balance > 0 THEN 1 END) AS positive_count,
           AVG(COALESCE(n.balance, 0)) AS avg_balance
    FROM months AS m
    CROSS JOIN customers AS c
    LEFT JOIN monthly_net AS n
      ON n.customer_id = c.customer_id AND n.month_start = m.month_start
    GROUP BY m.month_start
),
-- Select the earliest month when multiple months share an extreme count.
highest AS (
    SELECT month_start, avg_balance FROM month_stats
    ORDER BY positive_count DESC, month_start ASC
    LIMIT 1
),
lowest AS (
    SELECT month_start, avg_balance FROM month_stats
    ORDER BY positive_count ASC, month_start ASC
    LIMIT 1
)
SELECT strftime('%Y-%m', h.month_start) AS highest_count_month,
       h.avg_balance AS highest_count_month_avg_balance,
       strftime('%Y-%m', l.month_start) AS lowest_count_month,
       l.avg_balance AS lowest_count_month_avg_balance,
       h.avg_balance - l.avg_balance AS average_balance_difference
FROM highest AS h
CROSS JOIN lowest AS l;
