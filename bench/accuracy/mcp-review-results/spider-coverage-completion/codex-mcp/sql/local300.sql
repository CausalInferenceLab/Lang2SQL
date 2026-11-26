WITH RECURSIVE daily_changes AS (
    SELECT
        customer_id,
        txn_date,
        SUM(CASE
            WHEN txn_type = 'deposit' THEN txn_amount
            WHEN txn_type IN ('purchase', 'withdrawal') THEN -txn_amount
        END) AS net_change
    FROM main.customer_transactions
    GROUP BY customer_id, txn_date
),
transaction_balances AS (
    SELECT
        customer_id,
        txn_date,
        LEAD(txn_date) OVER customer_dates AS next_txn_date,
        SUM(net_change) OVER customer_dates AS closing_balance
    FROM daily_changes
    WINDOW customer_dates AS (PARTITION BY customer_id ORDER BY txn_date)
),
daily_balances(customer_id, balance_date, next_txn_date, balance) AS (
    SELECT customer_id, txn_date, next_txn_date, MAX(0, closing_balance)
    FROM transaction_balances

    UNION ALL

    SELECT customer_id, DATE(balance_date, '+1 day'), next_txn_date, balance
    FROM daily_balances
    WHERE DATE(balance_date, '+1 day') < next_txn_date
),
customer_monthly_peaks AS (
    SELECT
        customer_id,
        STRFTIME('%Y-%m', balance_date) AS month,
        MAX(balance) AS peak_balance
    FROM daily_balances
    GROUP BY customer_id, month
)
SELECT month, SUM(peak_balance) AS monthly_total
FROM customer_monthly_peaks
GROUP BY month
ORDER BY month
LIMIT 1000
