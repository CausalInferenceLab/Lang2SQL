WITH daily_cities AS (
    SELECT insert_date, city_name,
           ROW_NUMBER() OVER (
               PARTITION BY insert_date ORDER BY city_id, city_name
           ) AS city_rank
    FROM main.cities
    WHERE country_code_2 = 'cn'
      AND insert_date >= '2021-07-01'
      AND insert_date < '2021-08-01'
),
streak_days AS (
    SELECT insert_date, city_name,
           JULIANDAY(insert_date) - ROW_NUMBER() OVER (ORDER BY insert_date) AS streak_id
    FROM daily_cities
    WHERE city_rank = 1
),
streak_lengths AS (
    SELECT insert_date, city_name,
           COUNT(*) OVER (PARTITION BY streak_id) AS days_in_streak
    FROM streak_days
)
SELECT insert_date,
       UPPER(SUBSTR(city_name, 1, 1)) || LOWER(SUBSTR(city_name, 2)) AS city_name
FROM streak_lengths
WHERE days_in_streak = (SELECT MIN(days_in_streak) FROM streak_lengths)
   OR days_in_streak = (SELECT MAX(days_in_streak) FROM streak_lengths)
ORDER BY insert_date
LIMIT 31
