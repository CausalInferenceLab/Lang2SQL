WITH combined AS (
  SELECT
    t.tree_id,
    t.borough,
    COALESCE(t.zip_city, i.zip_code) AS zip_code,
    i.median_income,
    i.mean_income
  FROM trees t
  LEFT JOIN income_trees i ON t.zip_city = i.zip_code
  WHERE
    (i.median_income > 0 AND i.mean_income > 0)
    AND t.borough IS NOT NULL
    AND TRIM(t.borough) <> ''
)
SELECT
  borough,
  COUNT(*) AS tree_count,
  AVG(mean_income) AS avg_mean_income
FROM combined
GROUP BY borough
ORDER BY tree_count DESC
LIMIT 3;
