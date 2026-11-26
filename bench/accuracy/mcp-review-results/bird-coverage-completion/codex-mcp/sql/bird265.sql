SELECT COUNT(*) AS q1_count
FROM qualifying AS q
JOIN drivers AS d ON d.driverId = q.driverId
WHERE d.code IS NULL
  AND d.forename IN (
      SELECT forename
      FROM drivers
      GROUP BY forename
      HAVING COUNT(*) > 1
  )
  AND 60 * CAST(substr(q.q1, 1, instr(q.q1, ':') - 1) AS INTEGER)
      + CAST(substr(q.q1, instr(q.q1, ':') + 1) AS REAL) > 120
LIMIT 1
