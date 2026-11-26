WITH care_settings(admission) AS (
    VALUES ('+'), ('-')
), qualifying_patients AS (
    SELECT p."ID", p."Admission"
    FROM "Patient" AS p
    WHERE EXISTS (
        SELECT 1
        FROM "Laboratory" AS l
        WHERE l."ID" = p."ID"
          AND CAST(l."RNP" AS REAL) > 16
    )
)
SELECT s.admission AS care_setting,
       COUNT(DISTINCT p."ID") AS patient_count
FROM care_settings AS s
LEFT JOIN qualifying_patients AS p ON p."Admission" = s.admission
GROUP BY s.admission
ORDER BY s.admission;
