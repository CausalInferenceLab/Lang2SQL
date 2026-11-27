WITH monthly_payments AS (
    SELECT customer_id,
           CAST(strftime('%Y', payment_date) AS INTEGER) * 12
             + CAST(strftime('%m', payment_date) AS INTEGER) AS month_number,
           SUM(CAST(ROUND(amount * 100) AS INTEGER)) AS total_cents
    FROM payment
    GROUP BY customer_id, month_number
), customer_changes AS (
    SELECT curr.customer_id,
           AVG(curr.total_cents - prev.total_cents) AS avg_monthly_change
    FROM monthly_payments AS curr
    JOIN monthly_payments AS prev
      ON prev.customer_id = curr.customer_id
     AND prev.month_number = curr.month_number - 1
    GROUP BY curr.customer_id
), ranked_customers AS (
    SELECT customer_id,
           DENSE_RANK() OVER (ORDER BY avg_monthly_change DESC) AS change_rank
    FROM customer_changes
)
SELECT c.first_name || ' ' || c.last_name AS full_name
FROM ranked_customers AS r
JOIN customer AS c ON c.customer_id = r.customer_id
WHERE r.change_rank = 1;
