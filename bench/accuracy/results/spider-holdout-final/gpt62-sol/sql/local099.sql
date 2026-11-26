WITH yash_chopra AS (
    SELECT DISTINCT TRIM(PID) AS director_id
    FROM Person
    WHERE TRIM(Name) = 'Yash Chopra' COLLATE NOCASE
), collaboration_counts AS (
    SELECT TRIM(c.PID) AS actor_id,
           TRIM(d.PID) AS director_id,
           COUNT(DISTINCT c.MID) AS film_count
    FROM M_Cast AS c
    JOIN M_Director AS d ON d.MID = c.MID
    GROUP BY TRIM(c.PID), TRIM(d.PID)
)
SELECT COUNT(DISTINCT y.actor_id) AS actor_count
FROM collaboration_counts AS y
JOIN yash_chopra AS p ON p.director_id = y.director_id
WHERE NOT EXISTS (
    SELECT 1
    FROM collaboration_counts AS other
    WHERE other.actor_id = y.actor_id
      AND other.director_id <> y.director_id
      AND other.film_count >= y.film_count
);
