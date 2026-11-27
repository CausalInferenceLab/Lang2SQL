SELECT
    s.School,
    s.County,
    s.District,
    s.City,
    f."Enrollment (K-12)",
    f."FRPM Count (K-12)",
    f."Percent (%) Eligible FRPM (K-12)",
    sat.NumTstTakr,
    sat.AvgScrRead,
    sat.AvgScrMath,
    sat.AvgScrWrite,
    sat.NumGE1500
FROM schools AS s
LEFT JOIN frpm AS f ON f.CDSCode = s.CDSCode
LEFT JOIN satscores AS sat ON sat.cds = s.CDSCode
WHERE s.CDSCode = '01612590137943';
