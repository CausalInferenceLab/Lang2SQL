SELECT
  "L1_model" AS model_category,
  COUNT(*) AS total_count
FROM model
WHERE "L1_model" IN ('Traditional', 'Stack')
GROUP BY "L1_model"
ORDER BY total_count DESC
LIMIT 1;
