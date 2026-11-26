WITH fastest_laps AS (
    SELECT c.name AS circuit_name, MIN(l.milliseconds) AS fastest_ms
    FROM circuits AS c
    JOIN races AS r ON r.circuitId = c.circuitId
    JOIN lapTimes AS l ON l.raceId = r.raceId
    WHERE c.country = 'Italy' AND l.milliseconds IS NOT NULL
    GROUP BY c.circuitId, c.name
)
SELECT circuit_name,
       printf('%d:%02d.%03d', fastest_ms / 60000,
              (fastest_ms / 1000) % 60, fastest_ms % 1000) AS fastest_lap_time
FROM fastest_laps
ORDER BY circuit_name
LIMIT 1000
