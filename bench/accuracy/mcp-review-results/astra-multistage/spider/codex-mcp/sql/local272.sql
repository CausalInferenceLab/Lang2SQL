WITH demand AS (
    SELECT ol.id AS order_line_id, ol.product_id, ol.qty,
           SUM(ol.qty) OVER (
               PARTITION BY ol.product_id
               ORDER BY ol.id
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           ) AS demand_end
    FROM main.orderlines AS ol
    WHERE ol.order_id = 423
),
supply AS (
    SELECT i.product_id, l.aisle, l.position, i.qty,
           SUM(i.qty) OVER (
               PARTITION BY i.product_id
               ORDER BY p.purchased, i.qty, i.id
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           ) AS supply_end
    FROM main.inventory AS i
    JOIN main.locations AS l ON l.id = i.location_id
    JOIN main.purchases AS p ON p.id = i.purchase_id
    WHERE l.warehouse = 1
      AND i.qty > 0
      AND i.product_id IN (SELECT product_id FROM demand)
)
SELECT d.order_line_id, d.product_id, s.aisle, s.position,
       MIN(d.demand_end, s.supply_end)
         - MAX(d.demand_end - d.qty, s.supply_end - s.qty) AS pick_qty
FROM demand AS d
JOIN supply AS s
  ON s.product_id = d.product_id
 AND s.supply_end > d.demand_end - d.qty
 AND d.demand_end > s.supply_end - s.qty
ORDER BY d.order_line_id, s.supply_end
LIMIT 1000
