WITH payments_by_order AS (
    SELECT order_id, SUM(payment_value) AS order_payment
    FROM order_payments
    GROUP BY order_id
)
SELECT
    COUNT(*) AS number_of_orders,
    AVG(p.order_payment) AS average_payment_per_order,
    MAX(1.0, (
        JULIANDAY(MAX(DATE(o.order_purchase_timestamp)))
        - JULIANDAY(MIN(DATE(o.order_purchase_timestamp)))
    ) / 7.0) AS customer_lifespan_weeks
FROM orders AS o
JOIN customers AS c ON c.customer_id = o.customer_id
LEFT JOIN payments_by_order AS p ON p.order_id = o.order_id
GROUP BY c.customer_unique_id
ORDER BY average_payment_per_order DESC, c.customer_unique_id
LIMIT 3;
