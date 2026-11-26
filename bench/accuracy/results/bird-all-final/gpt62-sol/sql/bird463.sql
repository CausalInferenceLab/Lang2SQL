SELECT DISTINCT p.ID
FROM Patient AS p
WHERE p.Admission = '-'
  AND EXISTS (
    SELECT 1
    FROM Laboratory AS l
    WHERE l.ID = p.ID
      AND (l.RBC <= 3.5 OR l.RBC >= 6.0)
  )
  AND EXISTS (
    SELECT 1
    FROM Laboratory AS l
    WHERE l.ID = p.ID
      AND CAST(l.RNP AS REAL) > 0
  )
ORDER BY p.ID;
