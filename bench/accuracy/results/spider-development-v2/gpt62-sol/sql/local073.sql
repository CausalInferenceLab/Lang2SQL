WITH orders AS (
    SELECT o.rowid AS row_id, o.order_id, o.customer_id, n.pizza_name,
           CASE WHEN n.pizza_name = 'Meatlovers' THEN 1 ELSE 2 END AS pizza_id,
           o.order_time, o.exclusions, o.extras
    FROM pizza_clean_customer_orders AS o
    JOIN pizza_names AS n ON n.pizza_id = o.pizza_id
), ingredient_occurrences AS (
    SELECT o.row_id, CAST(s.value AS INTEGER) AS topping_id
    FROM orders AS o
    JOIN pizza_recipes AS r ON r.pizza_id = o.pizza_id
    CROSS JOIN json_each('[' || r.toppings || ']') AS s
    WHERE NOT EXISTS (
        SELECT 1
        FROM json_each('[' || COALESCE(o.exclusions, '') || ']') AS x
        WHERE CAST(x.value AS INTEGER) = CAST(s.value AS INTEGER)
    )
    UNION ALL
    SELECT o.row_id, CAST(e.value AS INTEGER) AS topping_id
    FROM orders AS o
    CROSS JOIN json_each('[' || COALESCE(o.extras, '') || ']') AS e
), ingredient_counts AS (
    SELECT row_id, topping_id, COUNT(*) AS quantity
    FROM ingredient_occurrences
    GROUP BY row_id, topping_id
)
SELECT o.row_id, o.order_id, o.customer_id, o.pizza_name,
       o.pizza_name || ': ' || COALESCE(
           GROUP_CONCAT(
               CASE WHEN c.quantity > 1 THEN '2x' || t.topping_name
                    ELSE t.topping_name END,
               ', ' ORDER BY (c.quantity > 1) DESC, t.topping_name COLLATE NOCASE
           ), ''
       ) AS ingredients
FROM orders AS o
LEFT JOIN ingredient_counts AS c ON c.row_id = o.row_id
LEFT JOIN pizza_toppings AS t ON t.topping_id = c.topping_id
GROUP BY o.row_id, o.order_id, o.pizza_name, o.order_time, o.customer_id
ORDER BY o.row_id ASC;
