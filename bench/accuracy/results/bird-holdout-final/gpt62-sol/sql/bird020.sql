WITH county_records AS (
    SELECT CDSCode AS cds_code,
           substr(CDSCode, 1, 7) AS district_code,
           District AS district_name,
           DOC,
           School,
           AdmFName1,
           AdmFName2,
           AdmFName3
    FROM schools
    WHERE County IN ('Amador', 'Madera')
),
public_districts AS (
    SELECT DISTINCT district_code, district_name
    FROM county_records
    WHERE substr(cds_code, -7) = '0000000'
      AND DOC IN ('00', '52', '54', '56')
),
school_records AS (
    SELECT cds_code, district_code, AdmFName1, AdmFName2, AdmFName3
    FROM county_records
    WHERE substr(cds_code, -7) <> '0000000'
      AND NULLIF(trim(School), '') IS NOT NULL
),
school_admin_names AS (
    SELECT cds_code, district_code, NULLIF(trim(AdmFName1), '') AS first_name
    FROM school_records
    UNION
    SELECT cds_code, district_code, NULLIF(trim(AdmFName2), '')
    FROM school_records
    UNION
    SELECT cds_code, district_code, NULLIF(trim(AdmFName3), '')
    FROM school_records
),
top_names AS (
    SELECT first_name, COUNT(*) AS total_schools
    FROM school_admin_names
    WHERE first_name IS NOT NULL
    GROUP BY first_name
    ORDER BY total_schools DESC, first_name ASC
    LIMIT 2
)
SELECT d.district_name,
       t.first_name,
       COUNT(a.cds_code) AS school_count
FROM public_districts AS d
CROSS JOIN top_names AS t
LEFT JOIN school_admin_names AS a
    ON a.district_code = d.district_code
   AND a.first_name = t.first_name
GROUP BY d.district_code, d.district_name, t.first_name, t.total_schools
ORDER BY d.district_name, d.district_code, t.total_schools DESC, t.first_name;
