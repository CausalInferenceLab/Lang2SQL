WITH middle_schools AS (
    SELECT f."School Type",
           COALESCE(f."School Name", s.School) AS "School Name",
           s.Latitude
    FROM schools AS s
    LEFT JOIN frpm AS f ON f.CDSCode = s.CDSCode
    WHERE s.State = 'CA'
      AND s.Latitude IS NOT NULL
      AND (
          f."School Type" IN ('Intermediate/Middle Schools (Public)', 'Junior High Schools (Public)')
          OR s.SOC IN ('62', '64')
          OR (' ' || f."School Name" || ' ') LIKE '% middle %'
          OR f."School Name" LIKE '%intermediate%'
          OR f."School Name" LIKE '%junior high%'
          OR (' ' || s.School || ' ') LIKE '% middle %'
          OR s.School LIKE '%intermediate%'
          OR s.School LIKE '%junior high%'
      )
)
SELECT "School Type", "School Name", Latitude
FROM middle_schools
WHERE Latitude = (SELECT MAX(Latitude) FROM middle_schools)
ORDER BY "School Name"
LIMIT -1
