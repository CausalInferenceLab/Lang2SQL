SELECT
    s.FundingType AS funding_category,
    COUNT(*) AS active_charter_schools,
    SUM(f."Enrollment (K-12)") AS total_k12_enrollment,
    ROUND(100.0 * AVG(f."Percent (%) Eligible FRPM (K-12)"), 2) AS avg_frpm_eligible_pct,
    ROUND(AVG(t.AvgScrRead + t.AvgScrMath + t.AvgScrWrite), 2) AS avg_combined_sat
FROM main.schools AS s
LEFT JOIN main.frpm AS f
    ON f.CDSCode = s.CDSCode
    AND f."Academic Year" = '2014-2015'
LEFT JOIN main.satscores AS t
    ON t.cds = s.CDSCode
WHERE s.StatusType = 'Active'
    AND s.Charter = 1
    AND s.FundingType IN ('Directly funded', 'Locally funded')
GROUP BY s.FundingType
ORDER BY s.FundingType
LIMIT 2
