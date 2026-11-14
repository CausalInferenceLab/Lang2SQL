SELECT pt.name AS ingredient_name, SUM(po.quantity) AS total_quantity
FROM pizza_customer_orders pco
JOIN pizza_recipes pr ON pco.pizza_id = pr.pizza_id
JOIN pizza_toppings pt ON pr.topping_id = pt.id
JOIN pizza_clean_customer_orders po ON pco.order_id = po.order_id AND pco.pizza_id = po.pizza_id
GROUP BY pt.name
ORDER BY total_quantity DESC;
