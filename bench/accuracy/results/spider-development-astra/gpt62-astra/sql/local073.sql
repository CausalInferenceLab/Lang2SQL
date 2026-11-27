WITH order_lines AS (
    SELECT o.rowid AS row_id, o.order_id, o.customer_id, n.pizza_name,
           o.order_time, o.exclusions, o.extras,
           CASE WHEN n.pizza_name = 'Meatlovers' THEN 1 ELSE 2 END AS pizza_id
    FROM pizza_clean_customer_orders AS o
    JOIN pizza_names AS n ON n.pizza_id = o.pizza_id
),
ingredients AS (
    SELECT o.row_id, CAST(s.value AS INTEGER) AS topping_id
    FROM order_lines AS o
    JOIN pizza_recipes AS r ON r.pizza_id = o.pizza_id
    CROSS JOIN json_each('[' || r.toppings || ']') AS s
    WHERE instr(',' || replace(coalesce(o.exclusions, ''), ' ', '') || ',',
                ',' || s.value || ',') = 0

    UNION ALL

    SELECT o.row_id, CAST(e.value AS INTEGER) AS topping_id
    FROM order_lines AS o
    CROSS JOIN json_each('["' || replace(o.extras, ',', '","') || '"]') AS e
),
topping_counts AS (
    SELECT i.row_id, t.topping_name, COUNT(*) AS qty
    FROM ingredients AS i
    JOIN pizza_toppings AS t ON t.topping_id = i.topping_id
    GROUP BY i.row_id, t.topping_name
)
SELECT o.row_id, o.order_id, o.customer_id, o.pizza_name,
       o.pizza_name || ': ' || coalesce((
           SELECT group_concat(label, ', ')
           FROM (
               SELECT CASE WHEN c.qty > 1 THEN '2x' ELSE '' END || c.topping_name AS label
               FROM topping_counts AS c
               WHERE c.row_id = o.row_id
               ORDER BY (c.qty > 1) DESC, c.topping_name COLLATE NOCASE
           )
       ), '') AS final_ingredients
FROM order_lines AS o
GROUP BY o.row_id, o.order_id, o.customer_id, o.pizza_name, o.order_time
ORDER BY o.row_id ASC;
