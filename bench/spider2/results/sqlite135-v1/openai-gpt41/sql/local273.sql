WITH fifo_inventory AS (
  SELECT
    i.product_id,
    i.location_id,
    i.available_qty,
    p.purchase_date,
    ROW_NUMBER() OVER (
      PARTITION BY i.product_id
      ORDER BY p.purchase_date ASC, i.available_qty ASC
    ) AS fifo_rank
  FROM inventory i
  JOIN purchases p ON i.purchase_id = p.id
),
order_requirements AS (
  SELECT
    ol.product_id,
    ol.order_id,
    ol.quantity AS required_qty
  FROM orderlines ol
),
matched_picks AS (
  SELECT
    o.product_id,
    o.order_id,
    o.required_qty,
    fi.location_id,
    fi.available_qty,
    fi.purchase_date,
    -- Picked qty is the min of required and available
    MIN(o.required_qty, fi.available_qty) AS picked_qty
  FROM order_requirements o
  JOIN fifo_inventory fi ON o.product_id = fi.product_id
),
pick_percentage AS (
  SELECT
    mp.product_id,
    -- Protect against division by zero
    CASE WHEN mp.required_qty > 0 THEN 
      CAST(mp.picked_qty AS FLOAT) / mp.required_qty
    ELSE 0 END AS pick_pct
  FROM matched_picks mp
)
SELECT
  p.name AS product_name,
  AVG(pp.pick_pct) AS avg_pick_percentage
FROM pick_percentage pp
JOIN products p ON pp.product_id = p.id
GROUP BY p.name
ORDER BY p.name
