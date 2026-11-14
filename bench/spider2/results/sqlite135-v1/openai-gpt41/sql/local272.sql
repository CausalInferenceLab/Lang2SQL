WITH order_lines AS (
    SELECT 
        o.id AS order_id,
        ol.id AS orderline_id,
        ol.product_id,
        ol.qty AS order_qty
    FROM orders o
    JOIN orderlines ol ON o.id = ol.order_id
    WHERE o.id = 423
),
inventory_1 AS (
    SELECT
        s.id AS stock_id,
        s.product_id,
        s.qty AS stock_qty,
        s.location_id,
        s.purchased AS purchased_date,
        l.aisle,
        l.position
    FROM stock s
    JOIN locations l ON s.location_id = l.id
    WHERE l.warehouse_id = 1 AND s.qty > 0
),
orderline_expanded AS (
    SELECT 
        ol.*, 
        SUM(ol.order_qty) OVER (PARTITION BY ol.product_id ORDER BY ol.orderline_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_order_qty,
        ROW_NUMBER() OVER (PARTITION BY ol.product_id ORDER BY ol.orderline_id) AS rn
    FROM order_lines ol
),
-- For each product, for each orderline, create a row for each location in inventory, ordered by purchased_date/qty asc
inventory_candidates AS (
    SELECT
        oe.order_id,
        oe.orderline_id,
        oe.product_id,
        oe.order_qty,
        oe.cum_order_qty,
        oe.rn,
        i1.stock_id,
        i1.location_id,
        i1.aisle,
        i1.position,
        i1.stock_qty,
        i1.purchased_date,
        ROW_NUMBER() OVER (PARTITION BY oe.product_id, oe.orderline_id ORDER BY i1.purchased_date ASC, i1.stock_qty ASC, i1.location_id ASC) AS inv_seq
    FROM orderline_expanded oe
    JOIN inventory_1 i1 ON oe.product_id = i1.product_id
),
recursive_pick AS (
    -- Base case: first orderline for each product
    SELECT
        order_id,
        orderline_id,
        product_id,
        order_qty,
        cum_order_qty,
        rn,
        stock_id,
        location_id,
        aisle,
        position,
        stock_qty,
        purchased_date,
        inv_seq,
        CASE 
            WHEN stock_qty >= order_qty THEN order_qty
            ELSE stock_qty
        END AS pick_qty,
        stock_qty - CASE 
                        WHEN stock_qty >= order_qty THEN order_qty
                        ELSE stock_qty
                    END AS remaining_stock,
        order_qty - CASE 
                        WHEN stock_qty >= order_qty THEN order_qty
                        ELSE stock_qty
                    END AS remaining_to_pick
    FROM inventory_candidates
    WHERE rn = 1 AND inv_seq = 1

    UNION ALL

    -- Recursive step: pick next inventory for the same orderline, or next orderline for this product
    SELECT
        ic.order_id,
        ic.orderline_id,
        ic.product_id,
        ic.order_qty,
        ic.cum_order_qty,
        ic.rn,
        ic.stock_id,
        ic.location_id,
        ic.aisle,
        ic.position,
        ic.stock_qty,
        ic.purchased_date,
        ic.inv_seq,
        -- Only pick what is still to pick, up to stock_qty
        CASE 
            WHEN prev.remaining_to_pick > ic.stock_qty THEN ic.stock_qty
            ELSE prev.remaining_to_pick
        END AS pick_qty,
        ic.stock_qty - CASE 
            WHEN prev.remaining_to_pick > ic.stock_qty THEN ic.stock_qty
            ELSE prev.remaining_to_pick
        END AS remaining_stock,
        prev.remaining_to_pick - CASE 
            WHEN prev.remaining_to_pick > ic.stock_qty THEN ic.stock_qty
            ELSE prev.remaining_to_pick
        END AS remaining_to_pick
    FROM inventory_candidates ic
    JOIN recursive_pick prev
        ON ic.product_id = prev.product_id 
        AND ic.orderline_id = prev.orderline_id
        AND ic.inv_seq = prev.inv_seq + 1
    WHERE prev.remaining_to_pick > 0

    UNION ALL

    -- Move to next orderline for the same product (if the previous orderline was fully satisfied)
    SELECT
        ic.order_id,
        ic.orderline_id,
        ic.product_id,
        ic.order_qty,
        ic.cum_order_qty,
        ic.rn,
        ic.stock_id,
        ic.location_id,
        ic.aisle,
        ic.position,
        ic.stock_qty,
        ic.purchased_date,
        ic.inv_seq,
        CASE 
            WHEN ic.stock_qty >= ic.order_qty THEN ic.order_qty
            ELSE ic.stock_qty
        END AS pick_qty,
        ic.stock_qty - CASE 
                            WHEN ic.stock_qty >= ic.order_qty THEN ic.order_qty
                            ELSE ic.stock_qty
                        END AS remaining_stock,
        ic.order_qty - CASE 
                            WHEN ic.stock_qty >= ic.order_qty THEN ic.order_qty
                            ELSE ic.stock_qty
                        END AS remaining_to_pick
    FROM inventory_candidates ic
    JOIN recursive_pick prev
        ON ic.product_id = prev.product_id
        AND ic.rn = prev.rn + 1
        AND ic.inv_seq = 1
    WHERE prev.remaining_to_pick = 0
),
-- Only include picks where something is picked
final_picks AS (
    SELECT
        order_id,
        product_id,
        orderline_id,
        location_id,
        aisle,
        position,
        pick_qty
    FROM recursive_pick
    WHERE pick_qty > 0
)
SELECT
    order_id,
    product_id,
    orderline_id,
    location_id,
    aisle,
    position,
    pick_qty
FROM final_picks
ORDER BY order_id, product_id, orderline_id, aisle, position
