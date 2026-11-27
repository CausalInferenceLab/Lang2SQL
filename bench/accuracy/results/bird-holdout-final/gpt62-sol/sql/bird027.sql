SELECT
    f."School Name" AS school_name,
    f."Low Grade" AS earliest_grade_level,
    f."Enrollment (K-12)" AS k12_enrollment,
    COALESCE(s.NumTstTakr, 0) AS sat_test_takers
FROM frpm AS f
LEFT JOIN satscores AS s
    ON s.cds = f.CDSCode AND s.rtype = 'S'
WHERE f."Academic Year" = '2014-2015'
  AND f."School Type" = 'State Special Schools'
ORDER BY
    CASE f."Low Grade"
        WHEN 'P' THEN -1
        WHEN 'K' THEN 0
        ELSE CAST(f."Low Grade" AS INTEGER)
    END DESC,
    f."Enrollment (K-12)" DESC,
    f."School Name" COLLATE NOCASE ASC
LIMIT 1;
