WITH locally_funded AS (
    SELECT f.CDSCode,
           f."Enrollment (K-12)" - f."Enrollment (Ages 5-17)" AS enrollment_difference
    FROM frpm AS f
    WHERE f."Charter Funding Type" = 'Locally funded'
)
SELECT s.School, s.DOCType
FROM locally_funded AS lf
JOIN schools AS s ON s.CDSCode = lf.CDSCode
WHERE lf.enrollment_difference > (
    SELECT AVG(enrollment_difference)
    FROM locally_funded
)
LIMIT 1000
