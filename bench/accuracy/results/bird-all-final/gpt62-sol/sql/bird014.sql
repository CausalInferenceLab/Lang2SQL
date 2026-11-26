SELECT s.School AS school_name, s.City AS city
FROM schools AS s
JOIN frpm AS f ON f.CDSCode = s.CDSCode
WHERE s.EILCode = 'HS'
  AND f."NSLP Provision Status" = 'Provision 2'
  AND CAST(f."Low Grade" AS INTEGER) > 9
ORDER BY s.School;
