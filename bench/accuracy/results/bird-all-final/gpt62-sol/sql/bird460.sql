SELECT COUNT(DISTINCT p.ID) AS patient_count
FROM Patient AS p
JOIN Examination AS e ON e.ID = p.ID
WHERE p.SEX = 'F'
  AND INSTR(',' || REPLACE(LOWER(TRIM(e.Diagnosis)), ', ', ',') || ',', ',sjs,') > 0;
