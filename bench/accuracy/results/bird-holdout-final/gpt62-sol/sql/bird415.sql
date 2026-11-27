SELECT COUNT(DISTINCT CASE WHEN p.SEX = 'F' THEN p.ID END)
     - COUNT(DISTINCT CASE WHEN p.SEX = 'M' THEN p.ID END) AS difference
FROM Patient AS p
WHERE p.Diagnosis = 'RA'
  AND EXISTS (
    SELECT 1
    FROM Laboratory AS l
    WHERE l.ID = p.ID AND l.UN < 30
  );
