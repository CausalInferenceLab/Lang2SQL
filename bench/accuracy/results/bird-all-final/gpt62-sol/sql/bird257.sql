WITH q2_times AS (
    SELECT q.driverId, q.q2
    FROM qualifying AS q
    JOIN races AS r ON r.raceId = q.raceId
    WHERE r.year = 2017
      AND r.name = 'Italian Grand Prix'
      AND q.q2 NOT IN ('', '\N')
)
SELECT d.nationality
FROM q2_times AS t
JOIN drivers AS d ON d.driverId = t.driverId
WHERE t.q2 = (SELECT MIN(q2) FROM q2_times);
