WITH speeds AS (
    SELECT
        MAX(CASE WHEN r.raceId = 853 THEN CAST(r.fastestLapSpeed AS REAL) END) AS speed_853,
        MAX(CASE WHEN r.raceId = 854 THEN CAST(r.fastestLapSpeed AS REAL) END) AS speed_854
    FROM results AS r
    JOIN drivers AS d ON d.driverId = r.driverId
    WHERE d.forename = 'Paul'
      AND d.surname = 'di Resta'
      AND r.raceId IN (853, 854)
)
SELECT ROUND(100.0 * (speed_853 - speed_854) / speed_854, 2) AS percentage_higher
FROM speeds;
