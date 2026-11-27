WITH RECURSIVE inventory_plan(product_id, mth, ending_qty, qty_minimum, qty_purchase) AS (
    SELECT i.product_id, '2018-12-01', i.qty, p.qty_minimum, p.qty_purchase
    FROM inventory_totals AS i
    JOIN product_minimums AS p ON p.product_id = i.product_id

    UNION ALL

    SELECT r.product_id, b.mth,
           r.ending_qty - b.qty
               + CASE WHEN r.ending_qty < r.qty_minimum THEN r.qty_purchase ELSE 0 END,
           r.qty_minimum, r.qty_purchase
    FROM inventory_plan AS r
    JOIN monthly_budget AS b
      ON b.product_id = r.product_id
     AND b.mth = date(r.mth, '+1 month')
    WHERE b.mth < '2020-01-01'
),
gaps AS (
    SELECT product_id, mth, ABS(ending_qty - qty_minimum) AS absolute_difference
    FROM inventory_plan
    WHERE mth >= '2019-01-01'
),
ranked AS (
    SELECT gaps.*, ROW_NUMBER() OVER (
        PARTITION BY product_id ORDER BY absolute_difference, mth
    ) AS rn
    FROM gaps
)
SELECT product_id, mth AS month, absolute_difference
FROM ranked
WHERE rn = 1
ORDER BY product_id;
