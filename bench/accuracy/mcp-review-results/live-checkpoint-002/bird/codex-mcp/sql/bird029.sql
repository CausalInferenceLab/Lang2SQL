SELECT
    f."Charter Funding Type" AS funding_type,
    COUNT(*) AS active_schools,
    SUM(f."Enrollment (K-12)") AS total_k12_enrollment,
    SUM(f."FRPM Count (K-12)") AS total_frpm_eligible_k12_students
FROM frpm AS f
JOIN schools AS s ON s.CDSCode = f.CDSCode
WHERE f."District Type" = 'County Office of Education (COE)'
  AND f."Charter School (Y/N)" = 1
  AND s.StatusType = 'Active'
  AND f."Charter Funding Type" IN ('Directly funded', 'Locally funded')
GROUP BY f."Charter Funding Type"
ORDER BY f."Charter Funding Type" ASC
LIMIT 1000
