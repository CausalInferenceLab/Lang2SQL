WITH september_stops AS (
    SELECT p.driverId, p.raceId, p.duration, p.milliseconds
    FROM pitStops AS p
    JOIN races AS r ON r.raceId = p.raceId
    WHERE r.date >= '2016-09-01' AND r.date < '2016-10-01'
)
SELECT d.forename || ' ' || d.surname AS driver_full_name,
       s.duration AS pit_stop_time,
       r.name AS race_name,
       c.name AS circuit_name,
       c.location AS circuit_location
FROM september_stops AS s
JOIN drivers AS d ON d.driverId = s.driverId
JOIN races AS r ON r.raceId = s.raceId
JOIN circuits AS c ON c.circuitId = r.circuitId
WHERE s.milliseconds = (SELECT MIN(milliseconds) FROM september_stops);
