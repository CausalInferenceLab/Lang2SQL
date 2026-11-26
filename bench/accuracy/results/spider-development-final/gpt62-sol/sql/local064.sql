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
               END) AS balance
    FROM customer_transactions
    WHERE txn_date >= '2020-01-01'
      AND txn_date < '2021-01-01'
      AND txn_type IN ('deposit', 'withdrawal')
    GROUP BY customer_id, date(txn_date, 'start of month')
), customer_month_balances AS (
    SELECT m.month_start,
           COALESCE(t.balance, 0) AS balance
    FROM months AS m
    CROSS JOIN customers AS c
    LEFT JOIN monthly_transactions AS t
      ON t.customer_id = c.customer_id
     AND t.month_start = m.month_start
), monthly_summary AS (
    SELECT month_start,
           COUNT(CASE WHEN balance > 0 THEN 1 END) AS positive_customer_count,
           AVG(balance) AS average_balance
    FROM customer_month_balances
    GROUP BY month_start
), extremes AS (
    SELECT MAX(positive_customer_count) AS highest_count,
           MIN(positive_customer_count) AS lowest_count
    FROM monthly_summary
)
SELECT strftime('%Y-%m', h.month_start) AS highest_count_month,
       strftime('%Y-%m', l.month_start) AS lowest_count_month,
       h.average_balance AS highest_count_month_average_balance,
       l.average_balance AS lowest_count_month_average_balance,
       h.average_balance - l.average_balance AS average_balance_difference
FROM extremes AS e
JOIN monthly_summary AS h ON h.positive_customer_count = e.highest_count
JOIN monthly_summary AS l ON l.positive_customer_count = e.lowest_count
ORDER BY h.month_start, l.month_start;
