WITH september_stops AS (
    SELECT p.driverId, p.milliseconds, r.name AS race_name, r.circuitId,
           MIN(p.milliseconds) OVER () AS fastest_ms
    FROM pitStops AS p
    JOIN races AS r ON r.raceId = p.raceId
    WHERE r.date >= '2016-09-01' AND r.date < '2016-10-01'
)
SELECT d.forename || ' ' || d.surname AS driver_name,
       s.milliseconds / 1000.0 AS pit_stop_seconds,
       s.race_name,
       c.name AS circuit_name,
       c.location AS circuit_location
FROM september_stops AS s
JOIN drivers AS d ON d.driverId = s.driverId
JOIN circuits AS c ON c.circuitId = s.circuitId
WHERE s.milliseconds = s.fastest_ms
LIMIT -1
