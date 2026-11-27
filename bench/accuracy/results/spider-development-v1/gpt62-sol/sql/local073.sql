WITH orders AS (
    SELECT c.rowid AS row_id, c.order_id, c.customer_id, n.pizza_name,
           CASE WHEN n.pizza_name = 'Meatlovers' THEN 1 ELSE 2 END AS pizza_id,
           c.order_time, c.exclusions, c.extras
    FROM pizza_clean_customer_orders AS c
    JOIN pizza_names AS n ON n.pizza_id = c.pizza_id
), ingredients AS (
    SELECT o.row_id, CAST(s.value AS INTEGER) AS topping_id
    FROM orders AS o
    JOIN pizza_recipes AS r ON r.pizza_id = o.pizza_id
    CROSS JOIN json_each('[' || r.toppings || ']') AS s
    WHERE NOT EXISTS (
        SELECT 1
        FROM json_each('[' || COALESCE(o.exclusions, '') || ']') AS e
        WHERE CAST(e.value AS INTEGER) = CAST(s.value AS INTEGER)
    )
    UNION ALL
    SELECT o.row_id, CAST(x.value AS INTEGER) AS topping_id
    FROM orders AS o
    CROSS JOIN json_each('[' || COALESCE(o.extras, '') || ']') AS x
), topping_counts AS (
    SELECT i.row_id, t.topping_name, COUNT(*) AS occurrences
    FROM ingredients AS i
    JOIN pizza_toppings AS t ON t.topping_id = i.topping_id
    GROUP BY i.row_id, t.topping_name
)
SELECT o.row_id, o.order_id, o.customer_id, o.pizza_id, o.pizza_name,
       o.pizza_name || ': ' || COALESCE((
           SELECT group_concat(label, ', ')
           FROM (
               SELECT CASE WHEN tc.occurrences > 1
                           THEN '2x' || tc.topping_name ELSE tc.topping_name END AS label
               FROM topping_counts AS tc
               WHERE tc.row_id = o.row_id
               ORDER BY (tc.occurrences > 1) DESC, tc.topping_name COLLATE NOCASE
           )
       ), '') AS ingredient_list
FROM orders AS o
GROUP BY o.row_id, o.order_id, o.pizza_name, o.order_time
ORDER BY o.row_id ASC;
