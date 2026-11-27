SELECT d.forename || ' ' || d.surname AS full_name
FROM drivers AS d
JOIN lapTimes AS l ON l.driverId = d.driverId
WHERE l.milliseconds IS NOT NULL
GROUP BY d.driverId, d.forename, d.surname
ORDER BY MIN(l.milliseconds), d.driverId
LIMIT 20;
