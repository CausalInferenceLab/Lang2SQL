WITH middle_schools AS (
    SELECT f."School Type", f."School Name", s.Latitude
    FROM frpm AS f
    JOIN schools AS s ON s.CDSCode = f.CDSCode
    WHERE s.State = 'CA'
      AND (
          f."School Type" IN ('Intermediate/Middle Schools (Public)', 'Junior High Schools (Public)')
          OR f."School Name" LIKE '%Middle%'
          OR f."School Name" LIKE '%Intermediate%'
          OR f."School Name" LIKE '%Junior High%'
      )
)
SELECT "School Type", "School Name", Latitude
FROM middle_schools
WHERE Latitude = (SELECT MAX(Latitude) FROM middle_schools);
