WITH RECURSIVE inventory_model(product_id, mth, ending_inventory, qty_minimum, qty_purchase) AS (
    SELECT i.product_id, '2018-12-01', i.qty, p.qty_minimum, p.qty_purchase
    FROM inventory_totals AS i
    JOIN product_minimums AS p ON p.product_id = i.product_id

    UNION ALL

    SELECT i.product_id,
           DATE(i.mth, '+1 month'),
           i.ending_inventory - COALESCE(o.qty, 0)
               + CASE
                     WHEN i.ending_inventory - COALESCE(o.qty, 0) < i.qty_minimum
                     THEN i.qty_purchase
                     ELSE 0
                 END,
           i.qty_minimum,
           i.qty_purchase
    FROM inventory_model AS i
    LEFT JOIN monthly_orders AS o
        ON o.product_id = i.product_id AND o.mth = DATE(i.mth, '+1 month')
    WHERE i.mth < '2019-12-01'
), ranked AS (
    SELECT product_id, mth,
           ABS(ending_inventory - qty_minimum) AS absolute_difference,
           ROW_NUMBER() OVER (
               PARTITION BY product_id
               ORDER BY ABS(ending_inventory - qty_minimum), mth
           ) AS rn
    FROM inventory_model
    WHERE mth >= '2019-01-01' AND mth < '2020-01-01'
)
SELECT product_id, mth, absolute_difference
FROM ranked
WHERE rn = 1
ORDER BY product_id;
