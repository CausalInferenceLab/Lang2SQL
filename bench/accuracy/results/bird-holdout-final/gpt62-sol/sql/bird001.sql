WITH local_enrollment AS (
    SELECT CDSCode,
           "Enrollment (K-12)" - "Enrollment (Ages 5-17)" AS enrollment_difference
    FROM frpm
    WHERE "Charter Funding Type" = 'Locally funded'
)
SELECT s.School, s.DOCType
FROM local_enrollment AS e
JOIN schools AS s ON s.CDSCode = e.CDSCode
WHERE e.enrollment_difference > (
    SELECT AVG(enrollment_difference)
    FROM local_enrollment
);
