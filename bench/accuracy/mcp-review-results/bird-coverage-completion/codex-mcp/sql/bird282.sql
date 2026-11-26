WITH season_q2 AS (
    SELECT q.driverId, r.name AS race_name, q.q2
    FROM qualifying AS q
    JOIN races AS r ON r.raceId = q.raceId
    WHERE r.year = 2017 AND q.q2 <> ''
)
SELECT d.surname, s.race_name
FROM season_q2 AS s
JOIN drivers AS d ON d.driverId = s.driverId
WHERE s.q2 = (SELECT MIN(q2) FROM season_q2)
ORDER BY d.surname, s.race_name
LIMIT 1000
