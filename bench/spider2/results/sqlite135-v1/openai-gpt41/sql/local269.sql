WITH RECURSIVE expanded_packaging AS (
    -- Base case: leaf items directly contained in packaging
    SELECT 
        pr.packaging_id AS root_packaging_id,
        pr.child_packaging_id AS leaf_packaging_id,
        pr.quantity AS qty
    FROM packaging_relations pr
    UNION ALL
    -- Recursive case: expand packaging relations
    SELECT 
        ep.root_packaging_id,
        pr.child_packaging_id,
        ep.qty * pr.quantity
    FROM expanded_packaging ep
    JOIN packaging_relations pr
        ON ep.leaf_packaging_id = pr.packaging_id
)
, all_leaves AS (
    -- Only include true leaves (i.e., those not containers in other packaging_relations)
    SELECT 
        ep.root_packaging_id,
        ep.leaf_packaging_id,
        SUM(ep.qty) AS total_quantity
    FROM expanded_packaging ep
    LEFT JOIN packaging_relations pr2
        ON ep.leaf_packaging_id = pr2.packaging_id
    WHERE pr2.packaging_id IS NULL
    GROUP BY ep.root_packaging_id, ep.leaf_packaging_id
)
, total_per_combination AS (
    SELECT 
        root_packaging_id,
        SUM(total_quantity) AS combination_total
    FROM all_leaves
    GROUP BY root_packaging_id
)
SELECT 
    AVG(combination_total) AS avg_total_quantity
FROM total_per_combination;
