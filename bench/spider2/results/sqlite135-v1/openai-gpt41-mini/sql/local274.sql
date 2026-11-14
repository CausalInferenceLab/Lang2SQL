WITH ordered_picking AS (
    SELECT
        pl.product_id,
        pl.qty,
        p.created
    FROM picking_line pl
    JOIN picking_list p ON pl.picklist_id = p.id
    WHERE pl.order_id = 421
    ORDER BY p.created ASC
),

average_units_fifo AS (
    SELECT
        product_id,
        AVG(qty) AS avg_units_picked
    FROM ordered_picking
    GROUP BY product_id
)

SELECT
    product_id,
    avg_units_picked
FROM average_units_fifo;
