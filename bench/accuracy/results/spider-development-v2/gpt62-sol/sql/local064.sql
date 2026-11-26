WITH RECURSIVE months(month_start) AS (
    VALUES ('2020-01-01')
    UNION ALL
    SELECT date(month_start, '+1 month')
    FROM months
    WHERE month_start < '2020-12-01'
), customers AS (
    SELECT DISTINCT customer_id
    FROM customer_nodes
), monthly_transactions AS (
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
), customer_month_balances AS (
    SELECT m.month_start, c.customer_id, COALESCE(t.balance, 0) AS balance
    FROM months AS m
    CROSS JOIN customers AS c
    LEFT JOIN monthly_transactions AS t
        ON t.customer_id = c.customer_id AND t.month_start = m.month_start
), monthly_summary AS (
    SELECT month_start,
           COUNT(CASE WHEN balance > 0 THEN 1 END) AS positive_customers,
           AVG(balance) AS avg_balance
    FROM customer_month_balances
    GROUP BY month_start
), ranked_months AS (
    SELECT month_start, avg_balance,
           DENSE_RANK() OVER (ORDER BY positive_customers DESC) AS highest_rank,
           DENSE_RANK() OVER (ORDER BY positive_customers ASC) AS lowest_rank
    FROM monthly_summary
)
SELECT strftime('%Y-%m', h.month_start) AS highest_count_month,
       h.avg_balance AS highest_count_avg_balance,
       strftime('%Y-%m', l.month_start) AS lowest_count_month,
       l.avg_balance AS lowest_count_avg_balance,
       h.avg_balance - l.avg_balance AS avg_balance_difference
FROM ranked_months AS h
CROSS JOIN ranked_months AS l
WHERE h.highest_rank = 1 AND l.lowest_rank = 1
ORDER BY h.month_start, l.month_start;
