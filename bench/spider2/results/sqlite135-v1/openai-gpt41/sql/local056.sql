WITH monthly_payments AS (
    SELECT
        customer_id,
        strftime('%Y-%m', payment_date) AS year_month,
        SUM(amount) AS monthly_amount
    FROM payment
    GROUP BY customer_id, year_month
),
monthly_changes AS (
    SELECT
        customer_id,
        year_month,
        monthly_amount,
        LAG(monthly_amount) OVER (PARTITION BY customer_id ORDER BY year_month) AS prev_monthly_amount
    FROM monthly_payments
),
change_amounts AS (
    SELECT
        customer_id,
        ABS(monthly_amount - prev_monthly_amount) AS abs_change
    FROM monthly_changes
    WHERE prev_monthly_amount IS NOT NULL
),
avg_changes AS (
    SELECT
        customer_id,
        AVG(abs_change) AS avg_monthly_change
    FROM change_amounts
    GROUP BY customer_id
)
SELECT
    c.first_name || ' ' || c.last_name AS full_name
FROM
    avg_changes ac
    JOIN customer c ON ac.customer_id = c.customer_id
ORDER BY
    ac.avg_monthly_change DESC
LIMIT 1;
