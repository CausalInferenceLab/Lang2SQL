WITH delivered_pizzas AS (
    SELECT c.pizza_id, c.exclusions, c.extras
    FROM pizza_clean_customer_orders AS c
    WHERE EXISTS (
        SELECT 1
        FROM pizza_clean_runner_orders AS r
        WHERE r.order_id = c.order_id
          AND r.cancellation IS NULL
    )
), ingredient_usage AS (
    SELECT base.value AS topping_id
    FROM delivered_pizzas AS p
    JOIN pizza_recipes AS r ON r.pizza_id = p.pizza_id
    CROSS JOIN json_each('[' || r.toppings || ']') AS base
    WHERE NOT EXISTS (
        SELECT 1
        FROM json_each('[' || COALESCE(p.exclusions, '') || ']') AS excluded
        WHERE excluded.value = base.value
    )
    UNION ALL
    SELECT extra.value AS topping_id
    FROM delivered_pizzas AS p
    CROSS JOIN json_each('[' || COALESCE(p.extras, '') || ']') AS extra
)
SELECT t.topping_name AS name, COUNT(*) AS quantity
FROM ingredient_usage AS u
JOIN pizza_toppings AS t ON t.topping_id = u.topping_id
GROUP BY t.topping_id, t.topping_name
ORDER BY name;
