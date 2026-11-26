WITH delivered_keys AS (
    SELECT DISTINCT d.delivery_order_id
    FROM main.deliveries AS d
    WHERE d.delivery_status = 'DELIVERED'
),
store_counts AS (
    SELECT s.store_id, s.store_name,
           COUNT(*) AS total_orders,
           COUNT(dk.delivery_order_id) AS delivered_orders
    FROM main.orders AS o
    JOIN main.stores AS s ON s.store_id = o.store_id
    LEFT JOIN delivered_keys AS dk ON dk.delivery_order_id = o.delivery_order_id
    GROUP BY s.store_id, s.store_name
)
SELECT store_id, store_name, total_orders,
       1.0 * delivered_orders / total_orders AS delivered_ratio
FROM store_counts
WHERE total_orders = (SELECT MAX(total_orders) FROM store_counts)
ORDER BY store_id
LIMIT 1000
