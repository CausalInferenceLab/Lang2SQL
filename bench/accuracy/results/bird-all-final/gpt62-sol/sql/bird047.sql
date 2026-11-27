SELECT s.School AS school_name,
       s.Street AS street_address,
       s.City AS city,
       s.State AS state,
       s.Zip AS zip_code
FROM frpm AS f
JOIN schools AS s ON s.CDSCode = f.CDSCode
WHERE f."County Name" = 'Los Angeles'
  AND f."School Type" IN (
      'High Schools (Public)',
      'Continuation High Schools',
      'High Schools In 1 School Dist. (Public)'
  )
  AND f."FRPM Count (Ages 5-17)" > 500
ORDER BY s.School, s.CDSCode;
