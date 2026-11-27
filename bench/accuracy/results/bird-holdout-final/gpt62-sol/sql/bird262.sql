SELECT COUNT(DISTINCT q.driverId) AS driver_count
FROM qualifying AS q
JOIN races AS r ON r.raceId = q.raceId
WHERE r.year = 2017
  AND r.name = 'Italian Grand Prix'
  AND INSTR(q.q2, ':') > 0
  AND 60 * CAST(SUBSTR(q.q2, 1, INSTR(q.q2, ':') - 1) AS INTEGER)
      + CAST(SUBSTR(q.q2, INSTR(q.q2, ':') + 1) AS REAL) < 100;
