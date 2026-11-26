SELECT s.School AS school_name,
       s.Street AS street_address
FROM frpm AS f
JOIN schools AS s ON s.CDSCode = f.CDSCode
WHERE NULLIF(TRIM(s.School), '') IS NOT NULL
  AND f."Enrollment (K-12)" - f."Enrollment (Ages 5-17)" > 30
  AND f."High Grade" IN ('K', '1', '2', '3', '4', '5', '6', '7', '8');
