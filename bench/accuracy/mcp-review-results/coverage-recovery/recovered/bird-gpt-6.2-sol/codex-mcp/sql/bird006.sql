SELECT s.School,
       f."FRPM Count (Ages 5-17)" / NULLIF(f."Enrollment (Ages 5-17)", 0) * 100.0
           AS "Percent (%) Eligible FRPM (Ages 5-17)"
FROM schools AS s
LEFT JOIN frpm AS f ON f.CDSCode = s.CDSCode
WHERE s.County = 'Los Angeles'
  AND s.GSserved = 'K-9'
ORDER BY s.School
LIMIT 1000
