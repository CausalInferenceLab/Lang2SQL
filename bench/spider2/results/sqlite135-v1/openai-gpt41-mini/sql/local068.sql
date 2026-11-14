WITH city_monthly AS (
  SELECT
    strftime('%Y', created_at) AS year,
    strftime('%m', created_at) AS month,
    COUNT(*) AS cities_added
  FROM cities
  WHERE year BETWEEN '2021' AND '2023'
    AND month IN ('04', '05', '06')
  GROUP BY year, month
),
monthly_running AS (
  SELECT
    cm1.year,
    cm1.month,
    cm1.cities_added,
    (SELECT SUM(cm2.cities_added)
     FROM city_monthly cm2
     WHERE cm2.month = cm1.month
       AND cm2.year <= cm1.year) AS running_total
  FROM city_monthly cm1
),
growth_calculations AS (
  SELECT
    mr.year,
    mr.month,
    mr.cities_added,
    mr.running_total,
    LAG(mr.cities_added) OVER (PARTITION BY mr.month ORDER BY mr.year) AS prev_year_total,
    LAG(mr.running_total) OVER (PARTITION BY mr.month ORDER BY mr.year) AS prev_year_running
  FROM monthly_running mr
)
SELECT
  year,
  month,
  cities_added,
  running_total,
  CASE
    WHEN prev_year_total IS NULL THEN NULL
    ELSE ROUND(100.0 * (cities_added - prev_year_total) / prev_year_total, 2)
  END AS monthly_growth_pct,
  CASE
    WHEN prev_year_running IS NULL THEN NULL
    ELSE ROUND(100.0 * (running_total - prev_year_running) / prev_year_running, 2)
  END AS running_growth_pct
FROM growth_calculations
WHERE year IN ('2022', '2023')
ORDER BY year, month;
