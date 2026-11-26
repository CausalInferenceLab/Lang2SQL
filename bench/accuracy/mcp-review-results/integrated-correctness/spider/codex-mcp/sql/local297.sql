WITH monthly_net AS (
    SELECT
        tx.customer_id,
        DATE(tx.txn_date, 'start of month') AS month_start,
        SUM(CASE tx.txn_type
            WHEN 'deposit' THEN tx.txn_amount
            WHEN 'withdrawal' THEN -tx.txn_amount
        END) AS net_amount
    FROM main.customer_transactions AS tx
    WHERE tx.txn_type IN ('deposit', 'withdrawal')
    GROUP BY tx.customer_id, DATE(tx.txn_date, 'start of month')
),
monthly_balances AS (
    SELECT
        customer_id,
        month_start,
        SUM(net_amount) OVER (
            PARTITION BY customer_id
            ORDER BY month_start
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS closing_balance,
        net_amount
    FROM monthly_net
),
latest_balances AS (
    SELECT
        closing_balance,
        closing_balance - net_amount AS prior_month_balance,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id ORDER BY month_start DESC
        ) AS month_rank
    FROM monthly_balances
),
customer_growth AS (
    SELECT
        CASE
            WHEN prior_month_balance = 0 THEN closing_balance * 100.0
            ELSE (closing_balance - prior_month_balance) * 100.0 / prior_month_balance
        END AS growth_rate
    FROM latest_balances
    WHERE month_rank = 1
)
SELECT
    100.0 * COUNT(CASE WHEN growth_rate > 5 THEN 1 END)
        / NULLIF(COUNT(*), 0) AS percentage_of_customers
FROM customer_growth
LIMIT 1
