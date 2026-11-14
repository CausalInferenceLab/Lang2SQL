WITH finished_orders AS (
    SELECT 
        s.hub_id, 
        o.order_created_month AS month, 
        o.order_created_year AS year, 
        COUNT(*) AS finished_count
    FROM orders o
    JOIN stores s ON o.store_id = s.store_id
    WHERE o.order_moment_finished IS NOT NULL
      AND o.order_created_year = 2023
      AND o.order_created_month IN (2,3)
    GROUP BY s.hub_id, o.order_created_year, o.order_created_month
),
month_compare AS (
    SELECT
        f1.hub_id,
        f1.finished_count AS feb_count,
        f2.finished_count AS mar_count
    FROM finished_orders f1
    JOIN finished_orders f2
      ON f1.hub_id = f2.hub_id
     AND f1.month = 2 AND f2.month = 3
)
SELECT 
    h.hub_id, h.hub_name, 
    mc.feb_count, mc.mar_count,
    ROUND(CAST(mc.mar_count - mc.feb_count AS FLOAT) / mc.feb_count * 100, 2) AS percent_increase
FROM month_compare mc
JOIN hubs h ON mc.hub_id = h.hub_id
WHERE CAST(mc.mar_count - mc.feb_count AS FLOAT) / mc.feb_count > 0.2;
