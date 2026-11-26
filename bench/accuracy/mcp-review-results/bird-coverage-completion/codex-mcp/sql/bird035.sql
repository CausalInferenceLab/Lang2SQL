SELECT
    s.School,
    s.County,
    s.District,
    s.City,
    f."Enrollment (K-12)",
    f."FRPM Count (K-12)",
    f."Percent (%) Eligible FRPM (K-12)",
    sat.NumTstTakr AS sat_test_takers,
    sat.AvgScrRead AS average_reading_score,
    sat.AvgScrMath AS average_math_score,
    sat.AvgScrWrite AS average_writing_score,
    sat.NumGE1500 AS test_takers_scoring_at_least_1500
FROM schools AS s
JOIN frpm AS f ON f.CDSCode = s.CDSCode
JOIN satscores AS sat ON sat.cds = s.CDSCode
WHERE s.CDSCode = '01612590137943'
LIMIT 1000
