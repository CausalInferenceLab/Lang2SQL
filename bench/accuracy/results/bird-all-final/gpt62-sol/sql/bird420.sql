WITH proteinuria AS (
    SELECT ID,
           CASE
               WHEN TRIM("U-PRO") LIKE '>=%'
                   THEN CAST(SUBSTR(TRIM("U-PRO"), 3) AS REAL)
               WHEN TRIM("U-PRO") LIKE '+%(%)'
                   THEN CAST(SUBSTR(TRIM("U-PRO"), INSTR(TRIM("U-PRO"), '(') + 1) AS REAL)
               ELSE CAST("U-PRO" AS REAL)
           END AS proteinuria_level
    FROM Laboratory
)
SELECT COUNT(DISTINCT CASE WHEN p.Admission = '+' THEN p.ID END) AS admitted_patients,
       COUNT(DISTINCT CASE WHEN p.Admission = '-' THEN p.ID END) AS outpatient_patients
FROM Patient AS p
WHERE TRIM(p.Diagnosis) = 'SLE'
  AND EXISTS (
      SELECT 1
      FROM proteinuria AS l
      WHERE l.ID = p.ID
        AND l.proteinuria_level >= 30
  );
