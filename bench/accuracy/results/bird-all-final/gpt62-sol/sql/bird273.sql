SELECT
    COUNT(DISTINCT CASE WHEN d.forename = 'Peter' THEN d.driverId END) AS Peter,
    COUNT(DISTINCT CASE WHEN d.forename = 'John' THEN d.driverId END) AS John
FROM drivers AS d
JOIN results AS r ON r.driverId = d.driverId
WHERE d.forename IN ('Peter', 'John')
  AND r.position = 1;
