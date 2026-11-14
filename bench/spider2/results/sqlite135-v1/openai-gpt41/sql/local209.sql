WITH order_counts AS (
  SELECT
    o.store_id,
    s.name AS store_name,
    COUNT(*) AS total_orders
  FROM orders o
  JOIN stores s ON o.store_id = s.id
  GROUP BY o.store_id, s.name
),
top_store AS (
  SELECT store_id, store_name, total_orders
  FROM order_counts
  ORDER BY total_orders DESC
  LIMIT 1
),
delivered_count AS (
  SELECT
    o.store_id,
    COUNT(*) AS delivered_orders
  FROM orders o
  JOIN deliveries d ON o.id = d.order_id
  WHERE d.status = 'DELIVERED'
  GROUP BY o.store_id
)
SELECT
  t.store_name,
  t.total_orders,
  IFNULL(d.delivered_orders, 0) AS delivered_orders,
  ROUND(CAST(IFNULL(d.delivered_orders, 0) AS REAL) / t.total_orders, 4) AS delivered_order_ratio
FROM top_store t
LEFT JOIN delivered_count d ON t.store_id = d.store_id;
