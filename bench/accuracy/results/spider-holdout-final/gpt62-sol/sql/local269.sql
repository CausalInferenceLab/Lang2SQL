WITH RECURSIVE final_combinations AS (
    SELECT p.id
    FROM packaging AS p
    WHERE EXISTS (
        SELECT 1 FROM packaging_relations AS r WHERE r.packaging_id = p.id
    )
      AND NOT EXISTS (
        SELECT 1 FROM packaging_relations AS r WHERE r.contains_id = p.id
    )
), expanded(root_id, item_id, quantity) AS (
    SELECT f.id, r.contains_id, r.qty
    FROM final_combinations AS f
    JOIN packaging_relations AS r ON r.packaging_id = f.id
    UNION ALL
    SELECT e.root_id, r.contains_id, e.quantity * r.qty
    FROM expanded AS e
    JOIN packaging_relations AS r ON r.packaging_id = e.item_id
), combination_totals AS (
    SELECT e.root_id, SUM(e.quantity) AS total_quantity
    FROM expanded AS e
    WHERE NOT EXISTS (
        SELECT 1 FROM packaging_relations AS r WHERE r.packaging_id = e.item_id
    )
    GROUP BY e.root_id
)
SELECT AVG(total_quantity) AS average_total_quantity
FROM combination_totals;
