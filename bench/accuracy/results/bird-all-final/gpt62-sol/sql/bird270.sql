WITH driver_best_laps AS (
    SELECT raceId, driverId, MIN(milliseconds) AS best_lap_ms
    FROM lapTimes
    WHERE milliseconds IS NOT NULL
    GROUP BY raceId, driverId
), ranked_laps AS (
    SELECT raceId, driverId,
           RANK() OVER (PARTITION BY raceId ORDER BY best_lap_ms) AS lap_rank
    FROM driver_best_laps
)
SELECT COUNT(DISTINCT r.driverId) AS driver_count
FROM ranked_laps AS r
JOIN drivers AS d ON d.driverId = r.driverId
WHERE r.lap_rank = 2
  AND d.dob >= '1976-01-01';
