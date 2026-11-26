WITH speeds AS (
    SELECT
        CAST(r853.fastestLapSpeed AS REAL) AS speed_853,
        CAST(r854.fastestLapSpeed AS REAL) AS speed_854
    FROM main.drivers AS d
    JOIN main.results AS r853 ON r853.driverId = d.driverId AND r853.raceId = 853
    JOIN main.results AS r854 ON r854.driverId = d.driverId AND r854.raceId = 854
    WHERE d.forename = 'Paul' AND d.surname = 'di Resta'
)
SELECT ROUND(100.0 * (speed_853 - speed_854) / NULLIF(speed_854, 0), 2) AS percentage_increase
FROM speeds
LIMIT 1000
