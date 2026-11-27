SELECT d.forename || ' ' || d.surname AS full_name
FROM lapTimes AS l
JOIN drivers AS d ON d.driverId = l.driverId
WHERE l.milliseconds IS NOT NULL
GROUP BY d.driverId, d.forename, d.surname
ORDER BY MIN(l.milliseconds) ASC, d.driverId ASC
LIMIT 20;
