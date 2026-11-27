WITH hub_order_counts AS (
    SELECT
        h.hub_id,
        h.hub_name,
        COUNT(CASE WHEN o.order_created_month = 2 THEN 1 END) AS february_orders,
        COUNT(CASE WHEN o.order_created_month = 3 THEN 1 END) AS march_orders
    FROM orders AS o
    JOIN stores AS s ON s.store_id = o.store_id
    JOIN hubs AS h ON h.hub_id = s.hub_id
    WHERE o.order_status = 'FINISHED'
      AND o.order_created_month IN (2, 3)
    GROUP BY h.hub_id, h.hub_name
)
SELECT hub_name
FROM hub_order_counts
WHERE february_orders > 0
  AND 5 * march_orders > 6 * february_orders
ORDER BY hub_name;
