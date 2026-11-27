WITH matching_schools AS (
    SELECT AdmEmail1, AdmEmail2, AdmEmail3
    FROM schools
    WHERE County = 'San Bernardino'
      AND City = 'San Bernardino'
      AND DOCType = 'Unified School District'
      AND SOCType = 'Intermediate/Middle Schools (Public)'
      AND OpenDate >= '2009-01-01'
      AND OpenDate < '2011-01-01'
), emails AS (
    SELECT TRIM(AdmEmail1) AS email FROM matching_schools
    UNION
    SELECT TRIM(AdmEmail2) FROM matching_schools
    UNION
    SELECT TRIM(AdmEmail3) FROM matching_schools
)
SELECT email
FROM emails
WHERE email IS NOT NULL AND email <> ''
ORDER BY email;
