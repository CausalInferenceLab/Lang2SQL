SELECT d.number, d.surname
FROM qualifying AS q
JOIN drivers AS d ON d.driverId = q.driverId
JOIN races AS r ON r.raceId = q.raceId
JOIN circuits AS c ON c.circuitId = r.circuitId
WHERE r.year = 2017
  AND r.name = 'Italian Grand Prix'
  AND c.circuitRef = 'monza'
  AND INSTR(q.q3, ':') > 0
  AND 60 * CAST(SUBSTR(q.q3, 1, INSTR(q.q3, ':') - 1) AS INTEGER)
      + CAST(SUBSTR(q.q3, INSTR(q.q3, ':') + 1) AS REAL) < 98.000;
