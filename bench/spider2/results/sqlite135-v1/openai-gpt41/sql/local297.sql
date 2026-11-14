WITH txn_monthly AS (
    SELECT
        customer_id,
        strftime('%Y-%m-01', transaction_date) AS month,
        SUM(CASE WHEN transaction_type = 'deposit' THEN amount
                 WHEN transaction_type = 'withdrawal' THEN -amount
                 ELSE 0 END) AS monthly_net
    FROM customer_transactions
    GROUP BY customer_id, month
),
txn_cumsum AS (
    SELECT
        customer_id,
        month,
        monthly_net,
        SUM(monthly_net) OVER (
            PARTITION BY customer_id
            ORDER BY month
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS closing_balance
    FROM txn_monthly
),
month_ranks AS (
    SELECT
        customer_id,
        month,
        closing_balance,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY month DESC) AS rn
    FROM txn_cumsum
),
current_prev_balances AS (
    SELECT
        c1.customer_id,
        c1.month AS curr_month,
        c1.closing_balance AS curr_balance,
        c2.closing_balance AS prev_balance
    FROM month_ranks c1
    LEFT JOIN month_ranks c2
        ON c1.customer_id = c2.customer_id
        AND c2.rn = 2
    WHERE c1.rn = 1
),
growth_calc AS (
    SELECT
        customer_id,
        curr_month,
        curr_balance,
        COALESCE(prev_balance, 0) AS prev_balance,
        CASE
            WHEN COALESCE(prev_balance,0) = 0 THEN
                CASE WHEN curr_balance = 0 THEN 0
                     ELSE curr_balance * 100
                END
            ELSE
                (curr_balance - prev_balance) * 1.0 / prev_balance * 100
        END AS growth_rate
    FROM current_prev_balances
)
SELECT
    ROUND(AVG(CASE WHEN growth_rate > 5 THEN 1.0 ELSE 0 END) * 100, 2) AS pct_customers_growth_gt_5
FROM growth_calc
;
