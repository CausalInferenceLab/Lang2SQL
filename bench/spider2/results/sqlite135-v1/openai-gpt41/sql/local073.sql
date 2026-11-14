WITH
-- Step 1: Build pizza_id mapping for orders (per customer order row)
order_pizzas AS (
  SELECT
    o.rowid AS row_id,
    o.order_id,
    o.customer_id,
    CASE
      WHEN pn.pizza_name = 'Meatlovers' THEN 1
      ELSE 2
    END AS pizza_id,
    pn.pizza_name,
    o.pizza_id AS orig_pizza_id,
    o.order_time
  FROM pizza_clean_customer_orders o
  JOIN pizza_names pn on o.pizza_id = pn.pizza_id
),
-- Step 2: Get standard toppings per pizza_id (from pizza_recipes)
std_toppings AS (
  SELECT
    pn.pizza_id,
    pt.pizza_topping_id,
    pt.topping_name
  FROM pizza_names pn
  JOIN pizza_recipes pr ON pn.pizza_id = pr.pizza_id
  JOIN pizza_toppings pt ON instr(',' || pr.toppings || ',', ',' || pt.pizza_topping_id || ',') > 0
),
-- Step 3: Get excluded toppings per order row
excluded AS (
  SELECT
    e.rowid AS row_id,
    pt.pizza_topping_id,
    pt.topping_name
  FROM pizza_get_exclusions e
  JOIN pizza_toppings pt ON instr(',' || ifnull(e.exclusions, '') || ',', ',' || pt.pizza_topping_id || ',') > 0
),
-- Step 4: Get extra toppings per order row
extras AS (
  SELECT
    e.rowid AS row_id,
    pt.pizza_topping_id,
    pt.topping_name
  FROM pizza_get_extras e
  JOIN pizza_toppings pt ON instr(',' || ifnull(e.extras, '') || ',', ',' || pt.pizza_topping_id || ',') > 0
),
-- Step 5: Build all occurrences per order row (resolve std/exclude/extra sets)
ingredients_cte AS (
  SELECT
    op.row_id,
    op.order_id,
    op.customer_id,
    op.pizza_name,
    op.original_pizza_id,
    op.pizza_id,
    op.order_time,
    st.pizza_topping_id,
    st.topping_name,
    1 AS position,
    'std' AS src
  FROM order_pizzas op
  JOIN std_toppings st ON op.pizza_id = st.pizza_id
  -- Exclusions will be handled in the final set

  UNION ALL

  SELECT
    op.row_id,
    op.order_id,
    op.customer_id,
    op.pizza_name,
    op.orig_pizza_id,
    op.pizza_id,
    op.order_time,
    ex.pizza_topping_id,
    ex.topping_name,
    2 AS position,
    'extra' AS src
  FROM order_pizzas op
  LEFT JOIN extras ex ON op.row_id = ex.row_id
  WHERE ex.pizza_topping_id IS NOT NULL
),
-- Remove excluded toppings from the base+extras union
final_ingredients AS (
  SELECT
    i.row_id,
    i.order_id,
    i.customer_id,
    i.pizza_name,
    i.pizza_id,
    i.order_time,
    i.pizza_topping_id,
    i.topping_name,
    COUNT(*) AS times_appeared
  FROM ingredients_cte i
  LEFT JOIN excluded e
    ON i.row_id = e.row_id AND i.pizza_topping_id = e.pizza_topping_id
  WHERE e.pizza_topping_id IS NULL
  GROUP BY i.row_id, i.pizza_topping_id
),
-- Compose string in requested format
ingredient_strings AS (
  SELECT
    f.row_id,
    f.order_id,
    f.customer_id,
    f.pizza_name,
    f.pizza_id,
    f.order_time,
    -- Compose string: multiple-occurrence first (with 2x), then singles, alph order
    TRIM(
      GROUP_CONCAT(
        CASE
          WHEN f.times_appeared > 1 THEN '2x ' || f.topping_name
          ELSE f.topping_name
        END,
        ', '
      )
    ) AS ingredients_str,
    SUM(CASE WHEN f.times_appeared > 1 THEN 1 ELSE 0 END) AS double_occurrences
  FROM (
    SELECT
      *,
      -- For sorting: 1 for double, 2 for single
      CASE WHEN times_appeared > 1 THEN 1 ELSE 2 END AS ord1,
      lower(topping_name) AS ord2
    FROM final_ingredients
  ) f
  GROUP BY f.row_id, f.order_id, f.customer_id, f.pizza_name, f.pizza_id, f.order_time
  ORDER BY f.row_id ASC, ord1 ASC, ord2 ASC
)
SELECT
  row_id,
  order_id,
  customer_id,
  pizza_name,
  pizza_name || ': ' || (
    SELECT
      GROUP_CONCAT(x.item, ', ')
    FROM (
      -- Inner sort: first double-occurrences (alpha), then singles (alpha)
      SELECT
        CASE
          WHEN times_appeared > 1 THEN '2x ' || topping_name
          ELSE topping_name
        END AS item,
        CASE WHEN times_appeared > 1 THEN 0 ELSE 1 END AS first_sort,
        lower(topping_name) AS second_sort
      FROM final_ingredients fi
      WHERE fi.row_id = isect.row_id
      ORDER BY first_sort, second_sort
    ) x
  ) AS final_ingredients
FROM ingredient_strings isect
ORDER BY row_id ASC;
