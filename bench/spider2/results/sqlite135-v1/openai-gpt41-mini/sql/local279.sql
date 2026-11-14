WITH RECURSIVE
monthly_inventory AS (
  -- base case: Dec 2018 inventory levels
  SELECT
    inventory.product_id,
    2018 AS year,
    12 AS month,
    inventory.stock AS ending_inventory
  FROM inventory
  WHERE inventory.year = 2018 AND inventory.month = 12

  UNION ALL

  -- recursive step: calculate inventory for following months
  SELECT
    mi.product_id,
    CASE WHEN mi.month = 12 THEN mi.year + 1 ELSE mi.year END AS year,
    CASE WHEN mi.month = 12 THEN 1 ELSE mi.month + 1 END AS month,
    -- ending_inventory calculation with restock if below minimum
    CASE
      WHEN mi.ending_inventory < pm.qty_minimum THEN pm.qty_minimum
      ELSE mi.ending_inventory
    END AS ending_inventory
  FROM monthly_inventory mi
  JOIN product_minimums pm ON mi.product_id = pm.product_id
  WHERE (CASE WHEN mi.month = 12 THEN mi.year + 1 ELSE mi.year END) = 2019
),

monthly_diff AS (
  SELECT
    mi.product_id,
    mi.year || '-' || printf('%02d', mi.month) AS year_month,
    ABS(mi.ending_inventory - pm.qty_minimum) AS diff
  FROM monthly_inventory mi
  JOIN product_minimums pm ON mi.product_id = pm.product_id
  WHERE mi.year = 2019
),

min_diff_month AS (
  SELECT
    product_id,
    year_month,
    diff,
    RANK() OVER (PARTITION BY product_id ORDER BY diff) AS diff_rank
  FROM monthly_diff
)

SELECT product_id, year_month AS month, diff
FROM min_diff_month
WHERE diff_rank = 1
ORDER BY product_id
;
