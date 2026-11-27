WITH q3_results AS (
    SELECT d.forename || ' ' || d.surname AS driver_full_name,
           r.name AS race_name,
           r.year AS race_year,
           q.q3 AS q3_time,
           60 * CAST(substr(q.q3, 1, instr(q.q3, ':') - 1) AS INTEGER)
              + CAST(substr(q.q3, instr(q.q3, ':') + 1) AS REAL) AS q3_seconds
    FROM qualifying AS q
    JOIN drivers AS d ON d.driverId = q.driverId
    JOIN races AS r ON r.raceId = q.raceId
    WHERE d.nationality = 'British'
      AND CAST(strftime('%Y', d.dob) AS INTEGER) > 1980
      AND instr(q.q3, ':') > 0
)
SELECT driver_full_name, race_name, race_year, q3_time
FROM q3_results
WHERE q3_seconds = (SELECT MIN(q3_seconds) FROM q3_results);
