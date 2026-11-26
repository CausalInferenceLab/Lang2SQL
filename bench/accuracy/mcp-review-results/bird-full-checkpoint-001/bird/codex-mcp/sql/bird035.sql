SELECT
    s.School,
    s.County,
    s.District,
    s.City,
    f."Enrollment (K-12)",
    f."FRPM Count (K-12)",
    f."Percent (%) Eligible FRPM (K-12)",
    t.NumTstTakr AS sat_test_takers,
    t.AvgScrRead AS average_reading_score,
    t.AvgScrMath AS average_math_score,
    t.AvgScrWrite AS average_writing_score,
    t.NumGE1500 AS test_takers_scoring_at_least_1500
FROM schools AS s
JOIN frpm AS f ON f.CDSCode = s.CDSCode
JOIN satscores AS t ON t.cds = s.CDSCode
WHERE s.CDSCode = '01612590137943'
LIMIT 1
