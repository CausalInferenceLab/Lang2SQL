SELECT d.nationality
FROM drivers AS d
WHERE d.driverId IN (
    SELECT q.driverId
    FROM qualifying AS q
    WHERE q.q3 = (SELECT MIN(NULLIF(q3, '')) FROM qualifying)
)
LIMIT 1000
