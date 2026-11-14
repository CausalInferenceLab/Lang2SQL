WITH jan_data AS (
  SELECT cc.country_id, cc.country_name, ci.city_id, ci.capital, ci.insert_date
  FROM cities ci
  JOIN cities_countries cc ON ci.country_code_2 = cc.country_code_2
  WHERE ci.insert_date BETWEEN '2022-01-01' AND '2022-01-31'
),

country_9_days AS (
  SELECT country_id
  FROM jan_data
  GROUP BY country_id
  HAVING COUNT(DISTINCT insert_date) = 9
),

country_dates AS (
  SELECT country_id, insert_date,
    ROW_NUMBER() OVER (PARTITION BY country_id ORDER BY insert_date) AS rn
  FROM jan_data
  WHERE country_id IN (SELECT country_id FROM country_9_days)
),

consecutive_periods AS (
  SELECT country_id, insert_date, rn,
    DATE(insert_date, '-' || rn || ' day') AS group_id
  FROM country_dates
),

grouped_periods AS (
  SELECT country_id, group_id, MIN(insert_date) AS start_date, MAX(insert_date) AS end_date, COUNT(*) AS consecutive_days
  FROM consecutive_periods
  GROUP BY country_id, group_id
),

longest_period AS (
  SELECT country_id, start_date, end_date, consecutive_days
  FROM grouped_periods
  ORDER BY consecutive_days DESC
  LIMIT 1
),

entries_in_longest_period AS (
  SELECT jd.*
  FROM jan_data jd
  JOIN longest_period lp ON jd.country_id = lp.country_id
  WHERE jd.insert_date BETWEEN lp.start_date AND lp.end_date
),

result AS (
  SELECT lp.country_id, lp.start_date, lp.end_date, lp.consecutive_days,
    COUNT(eilp.city_id) AS total_entries,
    SUM(CASE WHEN eilp.capital = 1 THEN 1 ELSE 0 END) AS capital_entries,
    ROUND(CAST(SUM(CASE WHEN eilp.capital = 1 THEN 1 ELSE 0 END) AS FLOAT) / COUNT(eilp.city_id), 4) AS capital_proportion
  FROM longest_period lp
  LEFT JOIN entries_in_longest_period eilp ON lp.country_id = eilp.country_id
  GROUP BY lp.country_id, lp.start_date, lp.end_date, lp.consecutive_days
)

SELECT country_id, start_date, end_date, consecutive_days, total_entries, capital_entries, capital_proportion
FROM result;
