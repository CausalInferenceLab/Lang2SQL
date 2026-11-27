WITH monthly_nets AS (
    SELECT customer_id,
           DATE(txn_date, 'start of month') AS month_start,
           SUM(CASE txn_type
                   WHEN 'deposit' THEN txn_amount
                   WHEN 'withdrawal' THEN -txn_amount
               END) AS monthly_net
    FROM customer_transactions
    WHERE txn_type IN ('deposit', 'withdrawal')
    GROUP BY customer_id, DATE(txn_date, 'start of month')
), monthly_balances AS (
    SELECT customer_id,
           month_start,
           SUM(monthly_net) OVER (
               PARTITION BY customer_id
               ORDER BY month_start
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           ) AS closing_balance
    FROM monthly_nets
), ranked_balances AS (
    SELECT closing_balance,
           LAG(closing_balance, 1, 0) OVER (
               PARTITION BY customer_id ORDER BY month_start
           ) AS prior_balance,
           ROW_NUMBER() OVER (
               PARTITION BY customer_id ORDER BY month_start DESC
           ) AS month_rank
    FROM monthly_balances
), latest_growth AS (
    SELECT CASE
               WHEN prior_balance = 0 THEN closing_balance * 100.0
               ELSE 100.0 * (closing_balance - prior_balance) / prior_balance
           END AS growth_pct
    FROM ranked_balances
    WHERE month_rank = 1
)
SELECT 100.0 * COUNT(CASE WHEN growth_pct > 5 THEN 1 END)
       / NULLIF(COUNT(*), 0) AS percentage_of_customers
FROM latest_growth;
