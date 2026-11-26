WITH collaborations AS (
    SELECT TRIM(c.PID) AS actor_id,
           TRIM(d.PID) AS director_id,
           COUNT(DISTINCT c.MID) AS film_count
    FROM main.M_Cast AS c
    JOIN main.M_Director AS d ON d.MID = c.MID
    WHERE TRIM(c.PID) <> '' AND TRIM(d.PID) <> ''
    GROUP BY TRIM(c.PID), TRIM(d.PID)
),
yash_chopra AS (
    SELECT TRIM(PID) AS director_id
    FROM main.Person
    WHERE TRIM(Name) = 'Yash Chopra'
),
qualifying_actors AS (
    SELECT actor_id
    FROM collaborations
    GROUP BY actor_id
    HAVING MAX(CASE WHEN director_id IN (SELECT director_id FROM yash_chopra)
                    THEN film_count ELSE 0 END)
         > MAX(CASE WHEN director_id IN (SELECT director_id FROM yash_chopra)
                    THEN 0 ELSE film_count END)
)
SELECT COUNT(*) AS actor_count
FROM qualifying_actors
LIMIT 1
