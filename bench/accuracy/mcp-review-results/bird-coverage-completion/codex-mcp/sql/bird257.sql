WITH ranked_q2 AS (
    SELECT q.driverId, RANK() OVER (ORDER BY q.q2) AS lap_rank
    FROM main.qualifying AS q
    JOIN main.races AS r ON r.raceId = q.raceId
    WHERE r.year = 2017
      AND r.name = 'Italian Grand Prix'
      AND q.q2 <> ''
)
SELECT d.nationality
FROM ranked_q2 AS q
JOIN main.drivers AS d ON d.driverId = q.driverId
WHERE q.lap_rank = 1
LIMIT 1000
