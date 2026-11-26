SELECT s.School, s.Street
FROM frpm AS f
JOIN schools AS s ON s.CDSCode = f.CDSCode
WHERE f."Low Grade" IN ('9', '10', '11', '12')
  AND f."Enrollment (K-12)" - f."Enrollment (Ages 5-17)" > 30
ORDER BY s.CDSCode ASC
LIMIT -1
