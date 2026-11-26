WITH country_dates AS (
    SELECT DISTINCT country_code_2, insert_date
    FROM main.cities
    WHERE insert_date >= '2022-06-01'
      AND insert_date < '2022-07-01'
),
grouped_dates AS (
    SELECT country_code_2,
           julianday(insert_date) - ROW_NUMBER() OVER (
               PARTITION BY country_code_2 ORDER BY insert_date
           ) AS streak_group
    FROM country_dates
),
streaks AS (
    SELECT country_code_2, COUNT(*) AS streak_days
    FROM grouped_dates
    GROUP BY country_code_2, streak_group
)
SELECT DISTINCT country_code_2
FROM streaks
WHERE streak_days = (SELECT MAX(streak_days) FROM streaks)
ORDER BY country_code_2
LIMIT 1000
