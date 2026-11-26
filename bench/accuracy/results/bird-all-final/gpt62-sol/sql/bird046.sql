SELECT s.City AS city, COUNT(DISTINCT s.CDSCode) AS magnet_school_count
FROM schools AS s
JOIN frpm AS f ON f.CDSCode = s.CDSCode
WHERE s.Magnet = 1
  AND f."NSLP Provision Status" = 'Provision 2'
GROUP BY s.City
ORDER BY magnet_school_count DESC, city;
