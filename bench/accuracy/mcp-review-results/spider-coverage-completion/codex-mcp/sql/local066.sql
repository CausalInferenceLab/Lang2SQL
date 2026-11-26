WITH delivered_pizzas AS (
    SELECT c.pizza_id, c.exclusions, c.extras
    FROM main.pizza_clean_customer_orders AS c
    WHERE c.order_id IN (
        SELECT r.order_id
        FROM main.pizza_clean_runner_orders AS r
        WHERE r.cancellation IS NULL AND r.pickup_time IS NOT NULL
    )
),
ingredients_used AS (
    SELECT topping.value AS topping_id
    FROM delivered_pizzas AS d
    JOIN main.pizza_recipes AS p ON p.pizza_id = d.pizza_id
    CROSS JOIN json_each('[' || p.toppings || ']') AS topping
    WHERE NOT EXISTS (
        SELECT 1
        FROM json_each('[' || d.exclusions || ']') AS excluded
        WHERE excluded.value = topping.value
    )
    UNION ALL
    SELECT extra.value AS topping_id
    FROM delivered_pizzas AS d
    CROSS JOIN json_each('[' || d.extras || ']') AS extra
)
SELECT t.topping_name AS ingredient_name, COUNT(*) AS quantity
FROM ingredients_used AS i
JOIN main.pizza_toppings AS t ON t.topping_id = i.topping_id
GROUP BY t.topping_id, t.topping_name
ORDER BY quantity DESC, ingredient_name
LIMIT 1000
