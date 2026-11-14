WITH valid_orders AS (
  SELECT
    o.order_id,
    o.pizza_id,
    o.exclusions,
    o.extras,
    o.order_time
  FROM pizza_customer_orders o
  JOIN pizza_runner_orders ro ON o.order_id = ro.order_id
  WHERE ro.cancellation IS NULL OR TRIM(ro.cancellation) = ''
),
pizza_prices AS (
  SELECT
    pizza_id,
    CASE
      WHEN pizza_id = 1 THEN 12   -- Meat Lovers
      WHEN pizza_id = 2 THEN 10   -- Vegetarian
      ELSE 0
    END AS base_price
  FROM pizza_names
),
extras_count AS (
  -- Count the number of extra toppings per order
  SELECT
    order_id,
    CASE
      WHEN extras IS NULL OR TRIM(extras) = '' THEN 0
      ELSE
        LENGTH(extras) - LENGTH(REPLACE(extras, ',', '')) + 1
    END AS extra_toppings
  FROM valid_orders
)
SELECT
  SUM(pp.base_price + ec.extra_toppings * 1) AS total_income
FROM valid_orders vo
JOIN pizza_prices pp ON vo.pizza_id = pp.pizza_id
JOIN extras_count ec ON vo.order_id = ec.order_id;
