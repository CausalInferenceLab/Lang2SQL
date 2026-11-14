WITH year_category_counts AS (
  SELECT 
    strftime('%Y', collision_date) AS year,
    pcf_violation_category,
    COUNT(*) AS count
  FROM collisions
  WHERE year IN ('2011', '2021')
  GROUP BY year, pcf_violation_category
),
most_common_2021 AS (
  SELECT pcf_violation_category
  FROM year_category_counts
  WHERE year = '2021'
  ORDER BY count DESC
  LIMIT 1
),
category_shares AS (
  SELECT 
    ycc.year,
    ycc.count * 1.0 / y.total AS share
  FROM year_category_counts ycc
  JOIN (
    SELECT year, SUM(count) AS total
    FROM year_category_counts
    GROUP BY year
  ) y ON ycc.year = y.year
  WHERE ycc.pcf_violation_category = (SELECT pcf_violation_category FROM most_common_2021)
)
SELECT 
  (SELECT share FROM category_shares WHERE year = '2021') -
  (SELECT share FROM category_shares WHERE year = '2011') AS percentage_point_change
