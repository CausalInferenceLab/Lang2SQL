WITH city_dates AS (
  SELECT 
    country_code_2,
    insert_date,
    ROW_NUMBER() OVER (PARTITION BY country_code_2 ORDER BY insert_date) AS rn1,
    DATE(insert_date, '-' || ROW_NUMBER() OVER (PARTITION BY country_code_2 ORDER BY insert_date) || ' day') AS grp
  FROM cities
  WHERE insert_date BETWEEN '2022-06-01' AND '2022-06-30'
), streaks AS (
  SELECT 
    country_code_2,
    grp,
    COUNT(*) AS streak_length
  FROM city_dates
  GROUP BY country_code_2, grp
), max_streaks AS (
  SELECT 
    country_code_2,
    MAX(streak_length) AS max_streak_length
  FROM streaks
  GROUP BY country_code_2
), max_overall AS (
  SELECT MAX(max_streak_length) AS longest_streak FROM max_streaks
)
SELECT country_code_2
FROM max_streaks, max_overall
WHERE max_streak_length = longest_streak
ORDER BY country_code_2
;
