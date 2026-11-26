WITH sle_tests AS (
    SELECT p.ID, p.Diagnosis, l.DNA, CAST(l.DNA AS REAL) AS numeric_result
    FROM main.Patient AS p
    JOIN main.Laboratory AS l ON l.ID = p.ID
    WHERE p.Diagnosis LIKE '%SLE%'
      AND p.Diagnosis NOT LIKE '%SLE susp%'
      AND l.DNA IS NOT NULL
)
SELECT DISTINCT ID, Diagnosis, DNA
FROM sle_tests
WHERE numeric_result = (SELECT MAX(numeric_result) FROM sle_tests)
ORDER BY ID
LIMIT -1
