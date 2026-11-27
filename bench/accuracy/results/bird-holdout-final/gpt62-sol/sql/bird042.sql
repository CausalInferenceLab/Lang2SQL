SELECT s.School, s.Street
FROM schools AS s
JOIN frpm AS f ON f.CDSCode = s.CDSCode
WHERE f."Low Grade" IN ('9', '10', '11', '12')
  AND f."Enrollment (K-12)" - f."Enrollment (Ages 5-17)" > 30
ORDER BY s.CDSCode ASC;
