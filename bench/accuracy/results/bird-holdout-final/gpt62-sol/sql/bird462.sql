WITH proteinuria AS (
    SELECT ID,
           CAST(CASE
               WHEN TRIM("U-PRO") IN ('-', 'TR') THEN '0'
               WHEN INSTR("U-PRO", '(') > 0
                   THEN SUBSTR("U-PRO", INSTR("U-PRO", '(') + 1)
               ELSE LTRIM("U-PRO", ' >=')
           END AS REAL) AS mg_dl
    FROM Laboratory
)
SELECT COUNT(DISTINCT e.ID) AS patient_count
FROM Examination AS e
WHERE e.Thrombosis = 0
  AND NOT EXISTS (
      SELECT 1
      FROM Examination AS other
      WHERE other.ID = e.ID AND other.Thrombosis <> 0
  )
  AND EXISTS (
      SELECT 1
      FROM Laboratory AS l
      WHERE l.ID = e.ID AND LOWER(TRIM(l.SM)) IN ('negative', '0')
  )
  AND EXISTS (
      SELECT 1
      FROM proteinuria AS p
      WHERE p.ID = e.ID AND p.mg_dl >= 30
  );
