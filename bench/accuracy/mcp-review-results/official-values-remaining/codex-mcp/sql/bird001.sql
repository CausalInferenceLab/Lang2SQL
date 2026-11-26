WITH locally_funded AS (
    SELECT s.School, s.DOCType,
           f."Enrollment (K-12)" - f."Enrollment (Ages 5-17)" AS enrollment_difference
    FROM main.schools AS s
    JOIN main.frpm AS f ON f.CDSCode = s.CDSCode
    WHERE s.FundingType = 'Locally funded'
)
SELECT School, DOCType
FROM locally_funded
WHERE enrollment_difference > (SELECT AVG(enrollment_difference) FROM locally_funded)
ORDER BY School, DOCType
LIMIT 1000
