WITH RECURSIVE leaf_items AS (
  -- Start from top-level packaging combinations (all distinct packaging_id)
  SELECT
    pr.packaging_id AS final_packaging_id,
    pr.contains_id AS leaf_packaging_id,
    pr.qty AS quantity
  FROM packaging_relations pr
  WHERE pr.contains_id NOT IN (SELECT packaging_id FROM packaging_relations)
  
  UNION ALL
  
  -- Recursively expand nested packaging relations
  SELECT
    li.final_packaging_id,
    pr.contains_id,
    li.quantity * pr.qty AS quantity
  FROM packaging_relations pr
  JOIN leaf_items li ON pr.packaging_id = li.leaf_packaging_id
),

leaf_quantity_sums AS (
  -- Sum quantities for each final packaging and leaf packaging
  SELECT
    final_packaging_id,
    leaf_packaging_id,
    SUM(quantity) AS total_quantity
  FROM leaf_items
  GROUP BY final_packaging_id, leaf_packaging_id
),

final_totals AS (
  -- Sum all leaf quantities per final packaging
  SELECT
    final_packaging_id,
    SUM(total_quantity) AS total_quantity_across_leaves
  FROM leaf_quantity_sums
  GROUP BY final_packaging_id
)

SELECT AVG(total_quantity_across_leaves) AS avg_total_quantity
FROM final_totals;
