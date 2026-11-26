SELECT
    f."Charter Funding Type" AS charter_funding_type,
    COUNT(*) AS active_school_count,
    SUM(f."Enrollment (K-12)") AS total_k12_enrollment,
    SUM(f."FRPM Count (K-12)") AS total_k12_frpm_eligible
FROM frpm AS f
JOIN schools AS s ON s.CDSCode = f.CDSCode
WHERE f."District Type" = 'County Office of Education (COE)'
  AND f."Charter School (Y/N)" = 1
  AND f."Charter Funding Type" IN ('Directly funded', 'Locally funded')
  AND s.StatusType = 'Active'
GROUP BY f."Charter Funding Type"
ORDER BY CASE f."Charter Funding Type" WHEN 'Directly funded' THEN 1 ELSE 2 END;
