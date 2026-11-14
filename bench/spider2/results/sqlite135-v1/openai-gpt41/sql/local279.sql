WITH
-- Get minimum required level per product
min_levels AS (
    SELECT product_id, minimum_quantity
    FROM product_minimums
),

-- December 2018 ending inventory: treat as starting value for Jan 2019
dec_inventory AS (
    SELECT product_id, quantity AS end_inventory
    FROM inventory
    WHERE month = '2018-12'
),

-- 2019 months
months AS (
    SELECT '2019-01' AS month UNION ALL
    SELECT '2019-02' UNION ALL
    SELECT '2019-03' UNION ALL
    SELECT '2019-04' UNION ALL
    SELECT '2019-05' UNION ALL
    SELECT '2019-06' UNION ALL
    SELECT '2019-07' UNION ALL
    SELECT '2019-08' UNION ALL
    SELECT '2019-09' UNION ALL
    SELECT '2019-10' UNION ALL
    SELECT '2019-11' UNION ALL
    SELECT '2019-12'
),

-- Raw inflow and outflow per product/month
flows AS (
    SELECT
        i.product_id,
        i.month,
        COALESCE(inventory_in, 0) AS inventory_in,
        COALESCE(inventory_out, 0) AS inventory_out
    FROM (
        SELECT DISTINCT product_id FROM dec_inventory
    ) p
    CROSS JOIN months m
    LEFT JOIN (
        SELECT product_id, month, SUM(inventory_in) AS inventory_in, SUM(inventory_out) AS inventory_out
        FROM inventory
        WHERE substr(month, 1, 4) = '2019'
        GROUP BY product_id, month
    ) i ON p.product_id = i.product_id AND m.month = i.month
),

-- Recursive monthly inventory simulation including restock if below min after outflow
inventory_sim AS (
    -- Anchor: Jan 2019 uses Dec 2018 ending
    SELECT
        f.product_id,
        f.month,
        1 AS step,
        d.end_inventory AS prev_end_inventory,
        f.inventory_in, f.inventory_out,
        NULL AS restock_qty,   -- unknown until calculated
        NULL AS restocked_this_month
    FROM flows f
    JOIN dec_inventory d ON f.product_id = d.product_id AND f.month = '2019-01'

    UNION ALL

    -- Recurrence: step through each month, using prior month's ending as start
    SELECT
        f.product_id,
        f.month,
        sim.step + 1,
        sim.end_inventory AS prev_end_inventory,
        f.inventory_in, f.inventory_out,
        NULL,
        NULL
    FROM inventory_sim sim
    JOIN flows f ON
        sim.product_id = f.product_id
        AND f.month = (
            SELECT month
            FROM months m
            WHERE m.month > sim.month
            ORDER BY m.month
            LIMIT 1
        )
    WHERE sim.step < 12
),
-- Now, actually calculate the restocked amount and ending inventory per month per product
monthly_calc AS (
    SELECT
        s.product_id,
        s.month,
        ml.minimum_quantity,
        s.prev_end_inventory + IFNULL(s.inventory_in, 0) - IFNULL(s.inventory_out, 0) AS after_outflow,
        CASE 
            WHEN (s.prev_end_inventory + IFNULL(s.inventory_in, 0) - IFNULL(s.inventory_out, 0)) < ml.minimum_quantity
                THEN ml.minimum_quantity - (s.prev_end_inventory + IFNULL(s.inventory_in, 0) - IFNULL(s.inventory_out, 0))
            ELSE 0
        END AS restock_qty,
        -- ending inventory: restock if needed
        CASE 
            WHEN (s.prev_end_inventory + IFNULL(s.inventory_in, 0) - IFNULL(s.inventory_out, 0)) < ml.minimum_quantity
                THEN ml.minimum_quantity
            ELSE (s.prev_end_inventory + IFNULL(s.inventory_in, 0) - IFNULL(s.inventory_out, 0))
        END AS end_inventory
    FROM inventory_sim s
    JOIN min_levels ml ON s.product_id = ml.product_id
),

-- Add the month step information for joinable order
month_steps AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY month) AS step,
        month
    FROM months
),

enumerated_sim AS (
    -- Attach the step numbers to monthly_calc
    SELECT
        m.product_id,
        m.month,
        ms.step,
        m.minimum_quantity,
        m.end_inventory
    FROM monthly_calc m
    JOIN month_steps ms ON m.month = ms.month
)

-- For each product, get the month where |ending inventory - min| is minimal in 2019
SELECT
    e.product_id,
    e.month,
    ABS(e.end_inventory - e.minimum_quantity) AS abs_difference
FROM (
    SELECT
        product_id,
        month,
        end_inventory,
        minimum_quantity,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY ABS(end_inventory - minimum_quantity) ASC, month ASC
        ) AS rn
    FROM enumerated_sim
) e
WHERE e.rn = 1
ORDER BY e.product_id
