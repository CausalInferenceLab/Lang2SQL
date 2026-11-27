SELECT COUNT(DISTINCT p.ID) AS patient_count
FROM Patient AS p
WHERE TRIM(p.Diagnosis) = 'MCTD'
  AND NOT EXISTS (
    SELECT 1
    FROM Examination AS e
    WHERE e.ID = p.ID
      AND TRIM(e.Diagnosis) NOT IN ('', 'MCTD')
  )
  AND EXISTS (
    SELECT 1
    FROM Laboratory AS l
    WHERE l.ID = p.ID
      AND l.PLT BETWEEN 100 AND 400
  )
  AND EXISTS (
    SELECT 1
    FROM Laboratory AS l
    WHERE l.ID = p.ID
      AND CAST(l.RNP AS REAL) > 5
  );
