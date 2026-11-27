WITH timed_laps AS (
    SELECT d.driverId, d.forename, d.surname, q.q3,
           60000 * CAST(SUBSTR(q.q3, 1, INSTR(q.q3, ':') - 1) AS INTEGER)
           + ROUND(1000 * CAST(SUBSTR(q.q3, INSTR(q.q3, ':') + 1) AS REAL)) AS lap_ms
    FROM qualifying AS q
    JOIN drivers AS d ON d.driverId = q.driverId
    WHERE d.nationality = 'Italian'
      AND INSTR(q.q3, ':') > 0
)
SELECT forename, surname, q3
FROM timed_laps
WHERE lap_ms = (SELECT MIN(lap_ms) FROM timed_laps)
GROUP BY driverId, forename, surname, q3
ORDER BY surname, forename;
