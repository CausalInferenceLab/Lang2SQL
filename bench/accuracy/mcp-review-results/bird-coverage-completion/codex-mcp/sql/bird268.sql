WITH driver_bests AS (
    SELECT d.forename, d.surname, MIN(q.q3) AS q3
    FROM main.drivers AS d
    JOIN main.qualifying AS q ON q.driverId = d.driverId
    WHERE d.nationality = 'Italian'
      AND q.q3 <> ''
    GROUP BY d.driverId, d.forename, d.surname
)
SELECT forename, surname, q3
FROM driver_bests
WHERE q3 = (SELECT MIN(q3) FROM driver_bests)
ORDER BY surname, forename
LIMIT 1000
