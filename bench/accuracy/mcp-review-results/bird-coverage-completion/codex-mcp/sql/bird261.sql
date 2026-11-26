WITH ranked_q2 AS (
    SELECT q.driverId,
           DENSE_RANK() OVER (ORDER BY q.q2) AS time_rank
    FROM main.qualifying AS q
    JOIN main.races AS r ON r.raceId = q.raceId
    WHERE r.year = 2017
      AND r.name = 'Italian Grand Prix'
      AND NULLIF(q.q2, '') IS NOT NULL
)
SELECT d.nationality
FROM ranked_q2 AS q
JOIN main.drivers AS d ON d.driverId = q.driverId
WHERE q.time_rank = 2
LIMIT 1000
