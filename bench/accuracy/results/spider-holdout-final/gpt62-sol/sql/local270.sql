WITH RECURSIVE contents (container_id, item_id, quantity) AS (
    SELECT p.id, r.contains_id, r.qty
    FROM packaging AS p
    JOIN packaging_relations AS r ON r.packaging_id = p.id
    WHERE NOT EXISTS (
        SELECT 1
        FROM packaging_relations AS parent
        WHERE parent.contains_id = p.id
    )

    UNION ALL

    SELECT c.container_id, r.contains_id, c.quantity * r.qty
    FROM contents AS c
    JOIN packaging_relations AS r ON r.packaging_id = c.item_id
), qualifying_items AS (
    SELECT container_id, item_id
    FROM contents
    GROUP BY container_id, item_id
    HAVING SUM(quantity) > 500
)
SELECT container.name AS container_name, item.name AS item_name
FROM qualifying_items AS q
JOIN packaging AS container ON container.id = q.container_id
JOIN packaging AS item ON item.id = q.item_id
ORDER BY container_name, item_name;
