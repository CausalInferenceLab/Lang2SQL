WITH monthly_payments AS (
    SELECT 
        p.customer_id,
        strftime('%Y-%m', p.payment_date) AS year_month,
        SUM(p.amount) AS month_amount
    FROM payment p
    GROUP BY p.customer_id, year_month
),
ranked_customers AS (
    SELECT 
        customer_id,
        SUM(month_amount) AS total_amount
    FROM monthly_payments
    GROUP BY customer_id
    ORDER BY total_amount DESC
    LIMIT 10
),
month_diffs AS (
    SELECT 
        mp.customer_id,
        mp.year_month,
        mp.month_amount,
        LAG(mp.month_amount) OVER (PARTITION BY mp.customer_id ORDER BY mp.year_month) AS prev_month_amount
    FROM monthly_payments mp
    INNER JOIN ranked_customers rc ON mp.customer_id = rc.customer_id
),
mo_changes AS (
    SELECT 
        customer_id,
        year_month,
        ROUND(ABS(month_amount - prev_month_amount), 2) AS mo_diff
    FROM month_diffs
    WHERE prev_month_amount IS NOT NULL
)
SELECT 
    c.customer_id, 
    c.first_name, 
    c.last_name, 
    m.year_month, 
    m.mo_diff
FROM mo_changes m
JOIN customer c ON m.customer_id = c.customer_id
ORDER BY m.mo_diff DESC
LIMIT 1;
