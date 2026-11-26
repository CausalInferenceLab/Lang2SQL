WITH appearance_years AS (
    SELECT DISTINCT
        TRIM(c.PID) AS actor_id,
        CAST(SUBSTR(TRIM(m.year), -4) AS INTEGER) AS film_year
    FROM main.M_Cast AS c
    JOIN main.Movie AS m ON m.MID = c.MID
    WHERE TRIM(c.PID) <> ''
),
career_gaps AS (
    SELECT
        actor_id,
        film_year - LAG(film_year) OVER (
            PARTITION BY actor_id ORDER BY film_year
        ) - 1 AS years_without_credit
    FROM appearance_years
),
qualifying_actors AS (
    SELECT actor_id
    FROM career_gaps
    GROUP BY actor_id
    HAVING COUNT(CASE WHEN years_without_credit > 3 THEN 1 END) = 0
)
SELECT COUNT(*) AS actor_count
FROM qualifying_actors
LIMIT 1
