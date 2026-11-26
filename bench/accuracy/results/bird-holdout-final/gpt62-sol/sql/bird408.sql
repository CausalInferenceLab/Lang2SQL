WITH diagnosed_patients AS (
    SELECT DISTINCT ID
    FROM Patient
    WHERE TRIM(Diagnosis) = 'Psoriatic arthritis' COLLATE NOCASE
)
SELECT COUNT(l.ID) AS total_laboratory_tests,
       COUNT(DISTINCT CASE WHEN l.ID IS NULL THEN p.ID END) AS patients_with_no_laboratory_tests
FROM diagnosed_patients AS p
LEFT JOIN Laboratory AS l ON l.ID = p.ID;
