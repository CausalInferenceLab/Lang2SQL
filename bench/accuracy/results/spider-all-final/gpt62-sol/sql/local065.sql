SELECT COALESCE(SUM(
    CASE p.pizza_name
        WHEN 'Meatlovers' THEN 12
        WHEN 'Vegetarian' THEN 10
    END
    + CASE
        WHEN TRIM(COALESCE(c.extras, '')) = '' THEN 0
        ELSE 1 + LENGTH(c.extras) - LENGTH(REPLACE(c.extras, ',', ''))
      END
), 0) AS total_income
FROM pizza_clean_customer_orders AS c
JOIN pizza_names AS p ON p.pizza_id = c.pizza_id
WHERE EXISTS (
    SELECT 1
    FROM pizza_clean_runner_orders AS r
    WHERE r.order_id = c.order_id
      AND r.cancellation IS NULL
);
