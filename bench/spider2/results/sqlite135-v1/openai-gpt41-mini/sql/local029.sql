WITH delivered_orders AS (
    SELECT o.customer_id, o.order_id
    FROM olist_orders o
    WHERE o.order_status = 'delivered'
),
customer_order_counts AS (
    SELECT customer_id, COUNT(order_id) AS delivered_order_count
    FROM delivered_orders
    GROUP BY customer_id
),
top_customers AS (
    SELECT customer_id
    FROM customer_order_counts
    ORDER BY delivered_order_count DESC
    LIMIT 3
),
customer_avg_payment AS (
    SELECT o.customer_id, AVG(p.payment_value) AS avg_payment_value
    FROM olist_orders o
    JOIN olist_order_payments p ON o.order_id = p.order_id
    WHERE o.customer_id IN (SELECT customer_id FROM top_customers)
    GROUP BY o.customer_id
),
customer_location AS (
    SELECT DISTINCT customer_id, customer_unique_id, customer_city, customer_state
    FROM olist_customers
    WHERE customer_id IN (SELECT customer_id FROM top_customers)
)
SELECT cl.customer_unique_id, coc.delivered_order_count, cap.avg_payment_value, cl.customer_city, cl.customer_state
FROM top_customers tc
JOIN customer_order_counts coc ON tc.customer_id = coc.customer_id
JOIN customer_avg_payment cap ON tc.customer_id = cap.customer_id
JOIN customer_location cl ON tc.customer_id = cl.customer_id
ORDER BY coc.delivered_order_count DESC;
