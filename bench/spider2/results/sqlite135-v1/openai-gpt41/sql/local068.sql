WITH month_data AS (
  SELECT
    CAST(strftime('%Y', insert_date) AS INTEGER) AS year,
    CAST(strftime('%m', insert_date) AS INTEGER) AS month,
    COUNT(city_id) AS cities_added
  FROM cities
  WHERE strftime('%Y', insert_date) IN ('2021','2022','2023') AND strftime('%m', insert_date) IN ('04','05','06')
  GROUP BY year, month
),
all_periods AS (
  SELECT 2021 as year, 4 as month UNION ALL
  SELECT 2021, 5 UNION ALL
  SELECT 2021, 6 UNION ALL
  SELECT 2022, 4 UNION ALL
  SELECT 2022, 5 UNION ALL
  SELECT 2022, 6 UNION ALL
  SELECT 2023, 4 UNION ALL
  SELECT 2023, 5 UNION ALL
  SELECT 2023, 6
),
filled_months AS (
  SELECT ap.year, ap.month, COALESCE(md.cities_added, 0) AS cities_added
  FROM all_periods ap
  LEFT JOIN month_data md ON ap.year = md.year AND ap.month = md.month
),
cum_month AS (
  SELECT
    year, month,
    cities_added,
    (SELECT SUM(cm2.cities_added) FROM filled_months cm2 WHERE cm2.month = fm.month AND cm2.year <= fm.year) AS running_total
  FROM filled_months fm
),
growths AS (
  SELECT
    year, month, cities_added, running_total,
    LAG(cities_added) OVER (PARTITION BY month ORDER BY year) AS prev_cities_added,
    LAG(running_total) OVER (PARTITION BY month ORDER BY year) AS prev_running_total
  FROM cum_month
)
SELECT
  year,
  month,
  cities_added,
  running_total,
  CASE WHEN prev_cities_added IS NOT NULL AND prev_cities_added != 0 THEN ROUND((cities_added - prev_cities_added)*100.0/prev_cities_added,1) ELSE NULL END AS month_yoy_growth_pct,
  CASE WHEN prev_running_total IS NOT NULL AND prev_running_total != 0 THEN ROUND((running_total - prev_running_total)*100.0/prev_running_total,1) ELSE NULL END AS cumulative_yoy_growth_pct
FROM growths
WHERE year IN (2022, 2023)
ORDER BY year, month
