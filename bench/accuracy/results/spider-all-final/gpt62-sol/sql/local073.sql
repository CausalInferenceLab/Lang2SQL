WITH RECURSIVE
order_lines AS (
    SELECT o.rowid AS row_id, o.order_id, o.customer_id, n.pizza_name,
           o.order_time, o.exclusions, o.extras,
           CASE WHEN n.pizza_name = 'Meatlovers' THEN 1 ELSE 2 END AS pizza_id
    FROM pizza_clean_customer_orders AS o
    JOIN pizza_names AS n ON n.pizza_id = o.pizza_id
),
topping_lists AS (
    SELECT o.row_id, 'recipe' AS kind, r.toppings AS csv
    FROM order_lines AS o
    JOIN pizza_recipes AS r ON r.pizza_id = o.pizza_id
    UNION ALL
    SELECT row_id, 'exclusion', exclusions FROM order_lines
    UNION ALL
    SELECT row_id, 'extra', extras FROM order_lines
),
split(row_id, kind, topping_id, rest) AS (
    SELECT row_id, kind, NULL, csv || ','
    FROM topping_lists
    WHERE csv IS NOT NULL AND TRIM(csv) <> ''
    UNION ALL
    SELECT row_id, kind,
           CAST(NULLIF(TRIM(SUBSTR(rest, 1, INSTR(rest, ',') - 1)), '') AS INTEGER),
           SUBSTR(rest, INSTR(rest, ',') + 1)
    FROM split
    WHERE rest <> ''
),
topping_counts AS (
    SELECT s.row_id, t.topping_name, COUNT(*) AS copies
    FROM split AS s
    JOIN pizza_toppings AS t ON t.topping_id = s.topping_id
    WHERE s.kind = 'extra'
       OR (s.kind = 'recipe' AND NOT EXISTS (
            SELECT 1
            FROM split AS x
            WHERE x.row_id = s.row_id
              AND x.kind = 'exclusion'
              AND x.topping_id = s.topping_id
       ))
    GROUP BY s.row_id, t.topping_name
),
formatted AS (
    SELECT row_id,
           GROUP_CONCAT(
               CASE WHEN copies > 1 THEN '2x' ELSE '' END || topping_name, ', '
           ) OVER (
               PARTITION BY row_id
               ORDER BY (copies > 1) DESC, topping_name COLLATE NOCASE
               ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
           ) AS topping_string
    FROM topping_counts
)
SELECT o.row_id, o.order_id, o.customer_id, o.pizza_name,
       o.pizza_name || ': ' || COALESCE(MAX(f.topping_string), '') AS ingredients
FROM order_lines AS o
LEFT JOIN formatted AS f ON f.row_id = o.row_id
GROUP BY o.row_id, o.order_id, o.customer_id, o.pizza_name, o.order_time
ORDER BY o.row_id ASC;
