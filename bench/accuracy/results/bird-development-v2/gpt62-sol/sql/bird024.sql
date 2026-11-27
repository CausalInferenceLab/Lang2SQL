WITH california_schools AS (
    SELECT f."School Type",
           COALESCE(f."School Name", s.School) AS "School Name",
           COALESCE(f."School Type", s.SOCType) AS type_for_classification,
           ' ' || LOWER(REPLACE(REPLACE(COALESCE(f."School Name", s.School), '-', ' '), '.', '')) || ' ' AS name_for_classification,
           s.Latitude
    FROM schools AS s
    LEFT JOIN frpm AS f ON f.CDSCode = s.CDSCode
    WHERE s.State = 'CA' AND s.Latitude IS NOT NULL
), middle_schools AS (
    SELECT "School Type", "School Name", Latitude
    FROM california_schools
    WHERE type_for_classification IN ('Intermediate/Middle Schools (Public)', 'Junior High Schools (Public)')
       OR name_for_classification LIKE '% middle %'
       OR name_for_classification LIKE '% intermediate %'
       OR name_for_classification LIKE '% junior high %'
       OR name_for_classification LIKE '% jr high %'
)
SELECT "School Type", "School Name", Latitude
FROM middle_schools
WHERE Latitude = (SELECT MAX(Latitude) FROM middle_schools);
