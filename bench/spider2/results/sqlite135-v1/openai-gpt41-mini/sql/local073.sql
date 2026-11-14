WITH order_base AS (
    SELECT
        o.rowid AS row_id,
        o.order_id,
        o.customer_id,
        CASE WHEN r.pizza_name = 'Meatlovers' THEN 1 ELSE 2 END AS pizza_id,
        r.pizza_name,
        o.order_time
    FROM pizza_customer_orders o
    JOIN pizza_recipes r ON o.pizza_id = r.pizza_id
),
std_toppings AS (
    SELECT
        r.pizza_name,
        t.topping_name
    FROM pizza_recipes r
    JOIN pizza_toppings t ON r.pizza_id = t.pizza_id
),
order_exclusions AS (
    SELECT
        e.order_id,
        e.topping_name
    FROM pizza_get_exclusions e
),
order_extras AS (
    SELECT
        x.order_id,
        x.topping_name
    FROM pizza_get_extras x
),
ingredients_all AS (
    SELECT
        o.row_id,
        o.order_id,
        o.pizza_name,
        st.topping_name,
        1 AS source_count
    FROM order_base o
    JOIN std_toppings st ON o.pizza_name = st.pizza_name
    WHERE NOT EXISTS (
        SELECT 1 FROM order_exclusions ex
        WHERE ex.order_id = o.order_id AND ex.topping_name = st.topping_name
    )
    UNION ALL
    SELECT
        o.row_id,
        o.order_id,
        o.pizza_name,
        x.topping_name,
        1 AS source_count
    FROM order_base o
    JOIN order_extras x ON o.order_id = x.order_id
),
ingredients_counted AS (
    SELECT
        row_id,
        order_id,
        pizza_name,
        topping_name,
        COUNT(*) AS cnt
    FROM ingredients_all
    GROUP BY row_id, order_id, pizza_name, topping_name
),
ingredients_tagged AS (
    SELECT
        row_id,
        order_id,
        pizza_name,
        CASE WHEN cnt > 1 THEN '2x' || topping_name ELSE topping_name END AS topping_display,
        CASE WHEN cnt > 1 THEN 0 ELSE 1 END AS occurrence_order
    FROM ingredients_counted
),
ingredients_ordered AS (
    SELECT
        row_id,
        order_id,
        pizza_name,
        topping_display
    FROM ingredients_tagged
    ORDER BY
        row_id,
        topping_display COLLATE NOCASE,
        occurrence_order,
        topping_display
),
grouped_ingredients AS (
    SELECT
        row_id,
        order_id,
        pizza_name,
        GROUP_CONCAT(topping_display, ', ') AS ingredients_string
    FROM (
        SELECT 
            row_id,
            order_id,
            pizza_name,
            topping_display,
            occurrence_order,
            ROW_NUMBER() OVER (
              PARTITION BY row_id, order_id 
              ORDER BY occurrence_order, topping_display COLLATE NOCASE
            ) AS rn
        FROM ingredients_tagged
    )
    GROUP BY row_id, order_id, pizza_name
)
SELECT
    o.row_id,
    o.order_id,
    o.customer_id,
    o.pizza_name,
    o.pizza_name || ': ' || gi.ingredients_string AS final_ingredients
FROM order_base o
JOIN grouped_ingredients gi ON o.row_id = gi.row_id AND o.order_id = gi.order_id AND o.pizza_name = gi.pizza_name
GROUP BY o.row_id, o.order_id, o.customer_id, o.pizza_name, o.order_time, gi.ingredients_string
ORDER BY o.row_id ASC;
