WITH order_payment_totals AS (
    SELECT order_id, SUM(payment_value) AS total_payment
    FROM order_payments
    GROUP BY order_id
), customer_stats AS (
    SELECT c.customer_unique_id,
           COUNT(*) AS number_of_orders,
           SUM(p.total_payment) / COUNT(*) AS average_payment_per_order,
           JULIANDAY(MAX(o.order_purchase_timestamp)) -
           JULIANDAY(MIN(o.order_purchase_timestamp)) AS lifespan_days
    FROM orders AS o
    JOIN customers AS c ON c.customer_id = o.customer_id
    LEFT JOIN order_payment_totals AS p ON p.order_id = o.order_id
    GROUP BY c.customer_unique_id
    HAVING SUM(p.total_payment) IS NOT NULL
)
SELECT customer_unique_id,
       number_of_orders,
       average_payment_per_order,
       CASE WHEN lifespan_days < 7.0 THEN 1.0
            ELSE lifespan_days / 7.0
       END AS customer_lifespan_weeks
FROM customer_stats
ORDER BY average_payment_per_order DESC, customer_unique_id
LIMIT 3;
