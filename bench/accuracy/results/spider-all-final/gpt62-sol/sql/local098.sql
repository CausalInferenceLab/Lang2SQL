WITH appearance_years AS (
    SELECT DISTINCT
        TRIM(c.PID) AS actor_id,
        CAST(SUBSTR(TRIM(m.year), -4) AS INTEGER) AS film_year
    FROM M_Cast AS c
    JOIN Movie AS m ON m.MID = c.MID
    WHERE TRIM(c.PID) <> ''
      AND SUBSTR(TRIM(m.year), -4) GLOB '[0-9][0-9][0-9][0-9]'
), consecutive_appearances AS (
    SELECT actor_id, film_year,
           LAG(film_year) OVER (PARTITION BY actor_id ORDER BY film_year) AS previous_year
    FROM appearance_years
)
SELECT COUNT(*) AS actor_count
FROM (
    SELECT actor_id
    FROM consecutive_appearances
    GROUP BY actor_id
    HAVING MAX(CASE WHEN film_year - previous_year > 4 THEN 1 ELSE 0 END) = 0
) AS qualifying_actors;
