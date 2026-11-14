SELECT
  pt.topping_name AS ingredient_name,
  SUM(1) AS total_quantity
FROM pizza_clean_customer_orders pcco
JOIN pizza_names pn ON pcco.pizza_id = pn.pizza_id
JOIN pizza_recipes pr ON pn.pizza_id = pr.pizza_id
JOIN pizza_toppings pt ON pr.topping_id = pt.topping_id
WHERE pcco.cancellation IS NULL
GROUP BY pt.topping_name
ORDER BY total_quantity DESC;
