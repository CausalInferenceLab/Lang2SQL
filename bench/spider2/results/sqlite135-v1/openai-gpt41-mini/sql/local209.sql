WITH store_order_counts AS (
  SELECT
    o.store_id,
    COUNT(*) AS total_orders
  FROM orders o
  GROUP BY o.store_id
),
max_store AS (
  SELECT store_id, total_orders
  FROM store_order_counts
  ORDER BY total_orders DESC
  LIMIT 1
),
store_delivered_counts AS (
  SELECT
    o.store_id,
    COUNT(CASE WHEN d.delivery_status = 'DELIVERED' THEN 1 END) AS delivered_orders
  FROM orders o
  LEFT JOIN deliveries d ON o.order_id = d.delivery_order_id
  GROUP BY o.store_id
)
SELECT
  s.store_id,
  s.total_orders,
  COALESCE(delivered_orders, 0) AS delivered_orders,
  CASE WHEN s.total_orders > 0 THEN CAST(COALESCE(delivered_orders, 0) AS FLOAT) / s.total_orders ELSE 0 END AS delivered_ratio
FROM max_store s
LEFT JOIN store_delivered_counts d ON s.store_id = d.store_id;
