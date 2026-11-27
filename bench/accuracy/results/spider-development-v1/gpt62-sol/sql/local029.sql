WITH delivered_orders AS (
    SELECT o.order_id, o.order_purchase_timestamp,
           c.customer_unique_id, c.customer_city, c.customer_state
    FROM olist_orders AS o
    JOIN olist_customers AS c ON c.customer_id = o.customer_id
    WHERE o.order_status = 'delivered'
), top_customers AS (
    SELECT customer_unique_id, COUNT(DISTINCT order_id) AS delivered_order_count
    FROM delivered_orders
    GROUP BY customer_unique_id
    ORDER BY delivered_order_count DESC, customer_unique_id
    LIMIT 3
), payment_averages AS (
    SELECT d.customer_unique_id, AVG(p.payment_value) AS average_payment_value
    FROM delivered_orders AS d
    JOIN top_customers AS t ON t.customer_unique_id = d.customer_unique_id
    JOIN olist_order_payments AS p ON p.order_id = d.order_id
    GROUP BY d.customer_unique_id
), ranked_locations AS (
    SELECT d.customer_unique_id, d.customer_city, d.customer_state,
           ROW_NUMBER() OVER (
               PARTITION BY d.customer_unique_id
               ORDER BY d.order_purchase_timestamp DESC, d.order_id DESC
           ) AS location_rank
    FROM delivered_orders AS d
    JOIN top_customers AS t ON t.customer_unique_id = d.customer_unique_id
)
SELECT t.customer_unique_id, t.delivered_order_count, p.average_payment_value,
       l.customer_city, l.customer_state
FROM top_customers AS t
LEFT JOIN payment_averages AS p ON p.customer_unique_id = t.customer_unique_id
JOIN ranked_locations AS l
    ON l.customer_unique_id = t.customer_unique_id AND l.location_rank = 1
ORDER BY t.delivered_order_count DESC, t.customer_unique_id;
