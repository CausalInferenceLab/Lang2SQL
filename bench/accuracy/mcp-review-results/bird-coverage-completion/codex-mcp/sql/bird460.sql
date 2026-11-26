SELECT COUNT(DISTINCT p.ID) AS patient_count
FROM main.Examination AS e
JOIN main.Patient AS p ON p.ID = e.ID
WHERE p.SEX = 'F'
  AND instr(',' || replace(trim(e.Diagnosis), ', ', ',') || ',', ',SjS,') > 0
LIMIT 1
