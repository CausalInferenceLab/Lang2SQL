WITH candidates AS (
    SELECT s.NCESSchool, f."Enrollment (Ages 5-17)" AS enrollment
    FROM main.schools AS s
    JOIN main.frpm AS f ON f.CDSCode = s.CDSCode
    WHERE s.CDSCode IN ('01751010130583', '01612910137588', '01611760134270')
)
SELECT NCESSchool
FROM candidates
WHERE enrollment = (SELECT MAX(enrollment) FROM candidates)
LIMIT 3
