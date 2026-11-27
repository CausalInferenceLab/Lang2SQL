SELECT d.forename || ' ' || d.surname AS full_name
FROM drivers AS d
JOIN lapTimes AS lt ON lt.driverId = d.driverId
WHERE lt.milliseconds IS NOT NULL
GROUP BY d.driverId, d.forename, d.surname
ORDER BY MIN(lt.milliseconds) ASC, d.driverId ASC
LIMIT 20;
