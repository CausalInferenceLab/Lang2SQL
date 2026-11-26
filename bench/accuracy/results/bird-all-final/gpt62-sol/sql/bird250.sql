WITH fastest_laps AS (
    SELECT c.circuitId, c.name, MIN(l.milliseconds) AS lap_ms
    FROM circuits AS c
    JOIN races AS r ON r.circuitId = c.circuitId
    JOIN lapTimes AS l ON l.raceId = r.raceId
    WHERE c.country = 'Italy'
      AND l.milliseconds IS NOT NULL
    GROUP BY c.circuitId, c.name
)
SELECT name AS circuit_name,
       printf('%d:%02d.%03d', lap_ms / 60000, (lap_ms / 1000) % 60, lap_ms % 1000) AS fastest_lap_time
FROM fastest_laps
ORDER BY circuit_name;
