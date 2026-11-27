WITH delivered AS (
    SELECT c.customer_unique_id, c.customer_city, c.customer_state, o.order_id,
           ROW_NUMBER() OVER (
               PARTITION BY c.customer_unique_id
               ORDER BY o.order_purchase_timestamp DESC, o.order_id DESC
           ) AS location_rank
    FROM olist_orders AS o
    JOIN olist_customers AS c ON c.customer_id = o.customer_id
    WHERE o.order_status = 'delivered'
)
SELECT d.customer_unique_id,
       COUNT(DISTINCT d.order_id) AS delivered_order_count,
       AVG(p.payment_value) AS average_payment_value,
       MAX(CASE WHEN d.location_rank = 1 THEN d.customer_city END) AS customer_city,
       MAX(CASE WHEN d.location_rank = 1 THEN d.customer_state END) AS customer_state
FROM delivered AS d
LEFT JOIN olist_order_payments AS p ON p.order_id = d.order_id
GROUP BY d.customer_unique_id
ORDER BY delivered_order_count DESC, d.customer_unique_id ASC
LIMIT 3;
