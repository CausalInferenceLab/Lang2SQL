WITH RECURSIVE expanded(final_id, item_id, quantity) AS (
    SELECT r.packaging_id, r.contains_id, r.qty
    FROM main.packaging_relations AS r
    WHERE NOT EXISTS (
        SELECT 1
        FROM main.packaging_relations AS parent
        WHERE parent.contains_id = r.packaging_id
    )

    UNION ALL

    SELECT e.final_id, r.contains_id, e.quantity * r.qty
    FROM expanded AS e
    JOIN main.packaging_relations AS r ON r.packaging_id = e.item_id
),
combination_totals AS (
    SELECT e.final_id, SUM(e.quantity) AS total_quantity
    FROM expanded AS e
    WHERE NOT EXISTS (
        SELECT 1
        FROM main.packaging_relations AS child
        WHERE child.packaging_id = e.item_id
    )
    GROUP BY e.final_id
)
SELECT AVG(total_quantity) AS average_total_quantity
FROM combination_totals
LIMIT 1
