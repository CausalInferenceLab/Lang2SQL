WITH category_counts AS (
    SELECT L1_model, COUNT(*) AS total_count
    FROM stack_ok
    GROUP BY L1_model
)
SELECT L1_model, total_count
FROM category_counts
WHERE total_count = (SELECT MAX(total_count) FROM category_counts)
ORDER BY L1_model;
