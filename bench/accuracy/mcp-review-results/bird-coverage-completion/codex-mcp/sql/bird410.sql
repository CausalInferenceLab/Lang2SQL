SELECT e.Symptoms AS symptom
FROM Examination AS e
JOIN Patient AS p ON p.ID = e.ID
WHERE e.Thrombosis > 0
  AND (' ' || REPLACE(UPPER(p.Diagnosis), 'SLE SUSP', '') || ' ')
      GLOB '*[^A-Z0-9]SLE[^A-Z0-9]*'
  AND TRIM(e.Symptoms) <> ''
GROUP BY e.Symptoms
ORDER BY COUNT(*) DESC, e.Symptoms ASC
LIMIT 1
