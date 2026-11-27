WITH RECURSIVE patient_diseases(patient_id, disease, remaining) AS (
    SELECT ID, '',
           REPLACE(REPLACE(UPPER(Diagnosis), CHAR(29), ','), 'SLE  SJS', 'SLE,SJS') || ','
    FROM Patient
    WHERE Diagnosis IS NOT NULL
    UNION ALL
    SELECT patient_id,
           TRIM(SUBSTR(remaining, 1, INSTR(remaining, ',') - 1)),
           SUBSTR(remaining, INSTR(remaining, ',') + 1)
    FROM patient_diseases
    WHERE remaining <> ''
)
SELECT e.Symptoms AS symptom
FROM Examination AS e
WHERE e.Thrombosis > 0
  AND NULLIF(TRIM(e.Symptoms), '') IS NOT NULL
  AND EXISTS (
      SELECT 1
      FROM patient_diseases AS d
      WHERE d.patient_id = e.ID
        AND d.disease IN ('SLE', 'SLE (DIFFUSE LE)')
  )
GROUP BY e.Symptoms
ORDER BY COUNT(*) DESC, symptom COLLATE NOCASE ASC, symptom ASC
LIMIT 1;
