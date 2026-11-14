WITH june_cities AS (
    SELECT
        cc.country_code,
        cd.date,
        ROW_NUMBER() OVER (PARTITION BY cc.country_code ORDER BY cd.date) as rn,
        DATE(cd.date, '-' || ROW_NUMBER() OVER (PARTITION BY cc.country_code ORDER BY cd.date) || ' days') as grp
    FROM cities AS c
    JOIN cities_countries AS cc ON c.city_id = cc.city_id
    JOIN legislation_date_dim AS cd ON c.inserted_date = cd.date
    WHERE cd.date >= '2022-06-01' AND cd.date <= '2022-06-30'
),
streaks AS (
    SELECT
        country_code,
        COUNT(*) as streak_len
    FROM june_cities
    GROUP BY country_code, grp
),
max_streaks AS (
    SELECT
        country_code,
        MAX(streak_len) as max_streak
    FROM streaks
    GROUP BY country_code
),
top_streak AS (
    SELECT MAX(max_streak) as top_streak FROM max_streaks
)
SELECT DISTINCT country_code
FROM max_streaks, top_streak
WHERE max_streak = top_streak.top_streak
