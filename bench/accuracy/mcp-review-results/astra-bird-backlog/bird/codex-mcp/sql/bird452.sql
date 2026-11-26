WITH exams AS (
    SELECT e.ID, e."Examination Date", e.Symptoms,
           REPLACE(REPLACE(REPLACE(TRIM(e.Diagnosis), '->', ','), '+', ','), ', ', ',') AS diseases
    FROM Examination AS e
    WHERE TRIM(e.Symptoms) <> ''
)
SELECT e.Symptoms, p.Diagnosis AS overall_diagnosis
FROM exams AS e
JOIN Patient AS p ON p.ID = e.ID
WHERE (',' || e.diseases || ',') LIKE '%,SLE,%'
  AND p.Birthday IS NOT NULL
ORDER BY p.Birthday DESC, p.ID ASC, e."Examination Date" DESC
LIMIT 1
