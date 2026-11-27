SELECT
    s.FundingType AS funding_category,
    COUNT(*) AS active_charter_school_count,
    SUM(f."Enrollment (K-12)") AS total_k12_enrollment_2014_2015,
    ROUND(100.0 * AVG(f."Percent (%) Eligible FRPM (K-12)"), 2) AS avg_k12_frpm_eligible_pct,
    ROUND(AVG(a.AvgScrRead + a.AvgScrMath + a.AvgScrWrite), 2) AS avg_combined_sat_score
FROM schools AS s
LEFT JOIN frpm AS f
    ON f.CDSCode = s.CDSCode
    AND f."Academic Year" = '2014-2015'
LEFT JOIN satscores AS a
    ON a.cds = s.CDSCode
    AND a.rtype = 'S'
WHERE s.StatusType = 'Active'
    AND s.Charter = 1
    AND s.FundingType IN ('Directly funded', 'Locally funded')
GROUP BY s.FundingType
ORDER BY s.FundingType;
