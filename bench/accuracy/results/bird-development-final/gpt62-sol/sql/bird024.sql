WITH middle_schools AS (
    SELECT SOCType, School, Latitude
    FROM schools
    WHERE State = 'CA'
      AND (SOCType LIKE '%Middle%' OR School LIKE '%Middle%')
)
SELECT SOCType AS "School Type", School AS "School Name", Latitude
FROM middle_schools
WHERE Latitude = (SELECT MAX(Latitude) FROM middle_schools);
