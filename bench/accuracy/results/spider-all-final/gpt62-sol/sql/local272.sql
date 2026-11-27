WITH order_demand AS (
    SELECT id AS order_line_id,
           product_id,
           COALESCE(SUM(qty) OVER (
               PARTITION BY product_id ORDER BY id
               ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
           ), 0) AS demand_start,
           SUM(qty) OVER (
               PARTITION BY product_id ORDER BY id
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           ) AS demand_end
    FROM orderlines
    WHERE order_id = 423
), warehouse_supply AS (
    SELECT i.product_id,
           l.id AS location_id,
           l.aisle,
           l.position,
           COALESCE(SUM(i.qty) OVER (
               fifo ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
           ), 0) AS supply_start,
           SUM(i.qty) OVER (
               fifo ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           ) AS supply_end
    FROM inventory AS i
    JOIN locations AS l ON l.id = i.location_id
    JOIN purchases AS p ON p.id = i.purchase_id
    WHERE l.warehouse = 1
      AND i.qty > 0
      AND i.product_id IN (SELECT product_id FROM order_demand)
    WINDOW fifo AS (PARTITION BY i.product_id ORDER BY p.purchased, i.qty, i.id)
)
SELECT d.order_line_id,
       d.product_id,
       s.aisle,
       s.position,
       SUM(MIN(d.demand_end, s.supply_end) - MAX(d.demand_start, s.supply_start)) AS pick_qty
FROM order_demand AS d
JOIN warehouse_supply AS s
  ON s.product_id = d.product_id
 AND MAX(d.demand_start, s.supply_start) < MIN(d.demand_end, s.supply_end)
GROUP BY d.order_line_id, d.product_id, s.location_id, s.aisle, s.position
ORDER BY d.order_line_id, MIN(s.supply_start), s.location_id;
