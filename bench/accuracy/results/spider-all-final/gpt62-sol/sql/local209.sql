WITH store_totals AS (
    SELECT s.store_id, s.store_name, COUNT(*) AS total_orders
    FROM orders AS o
    JOIN stores AS s ON s.store_id = o.store_id
    GROUP BY s.store_id, s.store_name
), top_stores AS (
    SELECT store_id, store_name, total_orders
    FROM store_totals
    WHERE total_orders = (SELECT MAX(total_orders) FROM store_totals)
), delivered AS (
    SELECT DISTINCT delivery_order_id
    FROM deliveries
    WHERE delivery_status = 'DELIVERED'
)
SELECT t.store_name,
       1.0 * COUNT(d.delivery_order_id) / t.total_orders AS delivered_order_ratio
FROM top_stores AS t
JOIN orders AS o ON o.store_id = t.store_id
LEFT JOIN delivered AS d ON d.delivery_order_id = o.delivery_order_id
GROUP BY t.store_id, t.store_name, t.total_orders
ORDER BY t.store_id;
