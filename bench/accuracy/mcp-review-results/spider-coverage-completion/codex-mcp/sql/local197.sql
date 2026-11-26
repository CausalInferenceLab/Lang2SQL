WITH monthly_payments AS (
    SELECT p.customer_id,
           DATE(p.payment_date, 'start of month') AS month_start,
           SUM(CAST(ROUND(p.amount * 100) AS INTEGER)) AS total_cents
    FROM main.payment AS p
    GROUP BY p.customer_id, DATE(p.payment_date, 'start of month')
),
top_customers AS (
    SELECT customer_id
    FROM monthly_payments
    GROUP BY customer_id
    ORDER BY SUM(total_cents) DESC, customer_id
    LIMIT 10
),
monthly_changes AS (
    SELECT curr.customer_id,
           curr.month_start,
           ABS(curr.total_cents - prev.total_cents) AS difference_cents
    FROM top_customers AS t
    JOIN monthly_payments AS curr ON curr.customer_id = t.customer_id
    JOIN monthly_payments AS prev
      ON prev.customer_id = curr.customer_id
     AND prev.month_start = DATE(curr.month_start, '-1 month')
)
SELECT c.first_name || ' ' || c.last_name AS customer_name,
       STRFTIME('%Y-%m', m.month_start) AS ending_month,
       ROUND(m.difference_cents / 100.0, 2) AS payment_difference
FROM monthly_changes AS m
JOIN main.customer AS c ON c.customer_id = m.customer_id
ORDER BY m.difference_cents DESC, m.month_start, m.customer_id
LIMIT 1
