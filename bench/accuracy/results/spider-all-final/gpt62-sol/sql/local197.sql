WITH monthly_payments AS (
    SELECT customer_id,
           DATE(payment_date, 'start of month') AS payment_month,
           SUM(CAST(ROUND(amount * 100) AS INTEGER)) AS total_cents
    FROM payment
    GROUP BY customer_id, DATE(payment_date, 'start of month')
), top_customers AS (
    SELECT customer_id
    FROM monthly_payments
    GROUP BY customer_id
    ORDER BY SUM(total_cents) DESC, customer_id
    LIMIT 10
), monthly_comparison AS (
    SELECT m.customer_id,
           m.payment_month,
           m.total_cents,
           LAG(m.payment_month) OVER customer_months AS previous_month,
           LAG(m.total_cents) OVER customer_months AS previous_total_cents
    FROM monthly_payments AS m
    JOIN top_customers AS t ON t.customer_id = m.customer_id
    WINDOW customer_months AS (
        PARTITION BY m.customer_id ORDER BY m.payment_month
    )
), differences AS (
    SELECT customer_id,
           payment_month,
           ABS(total_cents - previous_total_cents) AS difference_cents
    FROM monthly_comparison
    WHERE payment_month = DATE(previous_month, '+1 month')
)
SELECT c.first_name || ' ' || c.last_name AS customer_name,
       STRFTIME('%Y-%m', d.payment_month) AS payment_month,
       ROUND(d.difference_cents / 100.0, 2) AS payment_difference
FROM differences AS d
JOIN customer AS c ON c.customer_id = d.customer_id
WHERE d.difference_cents = (SELECT MAX(difference_cents) FROM differences)
ORDER BY d.payment_month, c.customer_id;
