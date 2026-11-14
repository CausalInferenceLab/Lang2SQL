WITH model_counts AS (
  SELECT
    L1_model,
    model_type,
    COUNT(*) AS count
  FROM (
    SELECT L1_model, 'traditional' AS model_type FROM model WHERE name NOT LIKE '%Stack%'
    UNION ALL
    SELECT L1_model, 'stack' AS model_type FROM model WHERE name LIKE '%Stack%'
  )
  GROUP BY L1_model, model_type
), total_counts AS (
  SELECT L1_model, SUM(count) AS total_count
  FROM model_counts
  GROUP BY L1_model
)
SELECT L1_model, total_count
FROM total_counts
ORDER BY total_count DESC
LIMIT 1;
