SELECT COUNT(DISTINCT q.driverId) AS driver_count
FROM qualifying AS q
JOIN races AS r ON r.raceId = q.raceId
WHERE r.date >= '1995-01-01'
  AND r.date < '1996-01-01'
  AND INSTR(q.q1, ':') > 0
  AND 60 * CAST(SUBSTR(q.q1, 1, INSTR(q.q1, ':') - 1) AS INTEGER)
      + CAST(SUBSTR(q.q1, INSTR(q.q1, ':') + 1) AS REAL) > 120;
