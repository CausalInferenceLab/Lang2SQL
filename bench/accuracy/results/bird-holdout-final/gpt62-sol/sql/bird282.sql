WITH season_q2 AS (
    SELECT q.driverId,
           r.name AS race_name,
           60 * CAST(SUBSTR(q.q2, 1, INSTR(q.q2, ':') - 1) AS INTEGER)
               + CAST(SUBSTR(q.q2, INSTR(q.q2, ':') + 1) AS REAL) AS q2_seconds
    FROM qualifying AS q
    JOIN races AS r ON r.raceId = q.raceId
    WHERE r.year = 2017
      AND INSTR(q.q2, ':') > 0
)
SELECT d.surname, s.race_name
FROM season_q2 AS s
JOIN drivers AS d ON d.driverId = s.driverId
WHERE s.q2_seconds = (SELECT MIN(q2_seconds) FROM season_q2)
ORDER BY d.surname, s.race_name;
