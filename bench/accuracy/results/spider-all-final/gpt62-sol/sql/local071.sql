WITH city_days AS (
    SELECT DISTINCT country_code_2, DATE(insert_date) AS inserted_day
    FROM cities
    WHERE DATE(insert_date) >= '2022-06-01'
      AND DATE(insert_date) < '2022-07-01'
      AND LENGTH(country_code_2) = 2
), streak_days AS (
    SELECT country_code_2,
           JULIANDAY(inserted_day) - ROW_NUMBER() OVER (
               PARTITION BY country_code_2 ORDER BY inserted_day
           ) AS streak_id
    FROM city_days
), streaks AS (
    SELECT country_code_2, COUNT(*) AS streak_length
    FROM streak_days
    GROUP BY country_code_2, streak_id
)
SELECT DISTINCT country_code_2
FROM streaks
WHERE streak_length = (SELECT MAX(streak_length) FROM streaks)
ORDER BY country_code_2;
