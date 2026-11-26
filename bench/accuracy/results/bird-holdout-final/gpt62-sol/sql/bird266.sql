SELECT
    d.driverRef,
    COALESCE(SUM(r.points), 0.0) AS total_points
FROM drivers AS d
LEFT JOIN results AS r
    ON r.driverId = d.driverId
    AND EXISTS (
        SELECT 1
        FROM lapTimes AS lt
        WHERE lt.driverId = r.driverId
          AND lt.raceId = r.raceId
    )
WHERE d.forename = 'Robert'
GROUP BY d.driverId, d.driverRef;
