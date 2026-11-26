SELECT
    COUNT(l.ID) AS total_laboratory_tests,
    COUNT(*) FILTER (WHERE l.ID IS NULL) AS patients_without_laboratory_tests
FROM main.Patient AS p
LEFT JOIN main.Laboratory AS l ON l.ID = p.ID
WHERE p.Diagnosis = 'Psoriatic arthritis' COLLATE NOCASE
LIMIT 1
