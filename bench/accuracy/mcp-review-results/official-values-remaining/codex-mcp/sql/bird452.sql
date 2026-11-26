SELECT e.Symptoms, p.Diagnosis AS overall_diagnosis
FROM Examination AS e
JOIN Patient AS p ON p.ID = e.ID
WHERE TRIM(e.Symptoms) <> ''
  AND p.Birthday IS NOT NULL
  AND (',' || REPLACE(REPLACE(REPLACE(e.Diagnosis, ' ', ''), '+', ','), '->', ',') || ',')
      LIKE '%,SLE,%'
ORDER BY p.Birthday DESC, p.ID ASC, e."Examination Date" DESC
LIMIT 1
