WITH top_customers AS (
    SELECT c.customer_unique_id, COUNT(DISTINCT o.order_id) AS delivered_order_count
    FROM olist_customers AS c
    JOIN olist_orders AS o ON o.customer_id = c.customer_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_unique_id
    ORDER BY delivered_order_count DESC, c.customer_unique_id
    LIMIT 3
), ranked_orders AS MATERIALIZED (
    SELECT c.customer_unique_id, c.customer_city, c.customer_state, o.order_id,
           ROW_NUMBER() OVER (
               PARTITION BY c.customer_unique_id
               ORDER BY o.order_purchase_timestamp DESC, o.order_id DESC
           ) AS location_rank
    FROM olist_customers AS c
    JOIN olist_orders AS o ON o.customer_id = c.customer_id
    WHERE o.order_status = 'delivered'
      AND c.customer_unique_id IN (SELECT customer_unique_id FROM top_customers)
), relevant_payments AS MATERIALIZED (
    SELECT order_id, payment_value
    FROM olist_order_payments
    WHERE order_id IN (SELECT order_id FROM ranked_orders)
)
SELECT t.customer_unique_id,
       AVG(p.payment_value) AS average_payment_value,
       MAX(CASE WHEN r.location_rank = 1 THEN r.customer_city END) AS customer_city,
       MAX(CASE WHEN r.location_rank = 1 THEN r.customer_state END) AS customer_state
FROM top_customers AS t
JOIN ranked_orders AS r ON r.customer_unique_id = t.customer_unique_id
LEFT JOIN relevant_payments AS p ON p.order_id = r.order_id
GROUP BY t.customer_unique_id, t.delivered_order_count
ORDER BY t.delivered_order_count DESC, t.customer_unique_id;
