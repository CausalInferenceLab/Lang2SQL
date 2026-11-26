WITH combined AS (
    SELECT t.tree_id,
           TRIM(t.boroname) AS borough,
           COALESCE(t.zipcode, i.zipcode) AS zipcode,
           i.Estimate_Median_income AS median_income,
           i.Estimate_Mean_income AS mean_income
    FROM trees AS t
    LEFT JOIN income_trees AS i ON t.zipcode = i.zipcode
    UNION
    SELECT t.tree_id,
           TRIM(t.boroname),
           COALESCE(t.zipcode, i.zipcode),
           i.Estimate_Median_income,
           i.Estimate_Mean_income
    FROM income_trees AS i
    LEFT JOIN trees AS t ON t.zipcode = i.zipcode
    WHERE t.zipcode IS NULL
)
SELECT borough,
       AVG(mean_income) AS average_mean_income
FROM combined
WHERE median_income > 0
  AND mean_income > 0
  AND borough IS NOT NULL
  AND borough <> ''
GROUP BY borough
ORDER BY COUNT(tree_id) DESC, borough
LIMIT 3;
