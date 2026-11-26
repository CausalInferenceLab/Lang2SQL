SELECT COUNT(DISTINCT q.driverId) AS driver_count
FROM main.qualifying AS q
JOIN main.races AS r ON r.raceId = q.raceId
WHERE r.date >= '1995-01-01'
  AND r.date < '1996-01-01'
  AND 60 * CAST(substr(q.q1, 1, instr(q.q1, ':') - 1) AS INTEGER)
      + CAST(substr(q.q1, instr(q.q1, ':') + 1) AS REAL) > 120
LIMIT 1
