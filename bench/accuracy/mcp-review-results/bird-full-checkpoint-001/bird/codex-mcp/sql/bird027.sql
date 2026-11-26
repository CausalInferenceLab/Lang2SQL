SELECT
    f."School Name",
    f."Low Grade",
    f."Enrollment (K-12)",
    COALESCE(sat.NumTstTakr, 0) AS "SAT Test Takers"
FROM frpm AS f
LEFT JOIN satscores AS sat ON sat.cds = f.CDSCode
WHERE f."Academic Year" = '2014-2015'
  AND f."School Type" = 'State Special Schools'
ORDER BY
    CASE f."Low Grade"
        WHEN 'P' THEN -1
        WHEN 'K' THEN 0
        ELSE CAST(f."Low Grade" AS INTEGER)
    END DESC,
    f."Enrollment (K-12)" DESC,
    f."School Name" ASC
LIMIT 1
