WITH RECURSIVE month_bounds AS (
    SELECT DATE(MIN(txn_date), 'start of month') AS first_month,
           DATE(MAX(txn_date), 'start of month') AS last_month
    FROM customer_transactions
), months(month_start, last_month) AS (
    SELECT DATE(first_month, '+1 month'), last_month
    FROM month_bounds
    WHERE first_month < last_month
    UNION ALL
    SELECT DATE(month_start, '+1 month'), last_month
    FROM months
    WHERE month_start < last_month
), customer_balances AS (
    SELECT m.month_start,
           t.customer_id,
           SUM(CASE t.txn_type
                   WHEN 'deposit' THEN t.txn_amount
                   WHEN 'purchase' THEN -t.txn_amount
                   WHEN 'withdrawal' THEN -t.txn_amount
               END) AS balance
    FROM months AS m
    JOIN customer_transactions AS t ON t.txn_date < m.month_start
    GROUP BY m.month_start, t.customer_id
)
SELECT month_start AS month,
       SUM(MAX(balance, 0)) AS previous_total_balance
FROM customer_balances
GROUP BY month_start
ORDER BY month_start;
