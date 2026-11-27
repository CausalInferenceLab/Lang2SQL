SELECT COUNT(*) AS qualifying_count
FROM qualifying AS q
JOIN drivers AS d ON d.driverId = q.driverId
WHERE d.code IS NULL
  AND EXISTS (
    SELECT 1
    FROM drivers AS other
    WHERE other.forename = d.forename
      AND other.driverId <> d.driverId
  )
  AND INSTR(q.q1, ':') > 0
  AND 60 * CAST(SUBSTR(q.q1, 1, INSTR(q.q1, ':') - 1) AS INTEGER)
      + CAST(SUBSTR(q.q1, INSTR(q.q1, ':') + 1) AS REAL) > 120;
