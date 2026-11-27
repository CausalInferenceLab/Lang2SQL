SELECT COUNT(DISTINCT p.ID) AS patient_count
FROM Patient AS p
WHERE p.SEX = 'F'
  AND EXISTS (
    SELECT 1
    FROM Laboratory AS l
    WHERE l.ID = p.ID
      AND l.SC170 IN ('negative', '0')
  )
  AND NOT EXISTS (
    SELECT 1
    FROM Examination AS e
    WHERE e.ID = p.ID
      AND e.Symptoms IS NOT NULL
  );
