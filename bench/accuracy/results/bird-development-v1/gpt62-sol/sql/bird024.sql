WITH school_records AS (
    SELECT f."School Type" AS school_type,
           COALESCE(f."School Type", s.SOCType) AS type_for_matching,
           COALESCE(f."School Name", s.School) AS school_name,
           s.Latitude AS latitude
    FROM schools AS s
    LEFT JOIN frpm AS f ON f.CDSCode = s.CDSCode
    WHERE s.State = 'CA' AND s.Latitude IS NOT NULL
), middle_schools AS (
    SELECT school_type, school_name, latitude
    FROM school_records
    WHERE type_for_matching IN ('Intermediate/Middle Schools (Public)', 'Junior High Schools (Public)')
       OR (' ' || LOWER(school_name) || ' ') GLOB '*[^a-z0-9]middle[^a-z0-9]*'
       OR (' ' || LOWER(school_name) || ' ') GLOB '*[^a-z0-9]intermediate[^a-z0-9]*'
       OR (' ' || LOWER(school_name) || ' ') GLOB '*[^a-z0-9]junior high[^a-z0-9]*'
)
SELECT school_type AS "School Type", school_name AS "School Name", latitude AS Latitude
FROM middle_schools
WHERE latitude = (SELECT MAX(latitude) FROM middle_schools)
ORDER BY school_name, school_type;
