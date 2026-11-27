SELECT ROUND(AVG(l.TP - 8.5), 3) AS average_tp_deviation
FROM Laboratory AS l
WHERE l.TP > 8.5
  AND EXISTS (
    SELECT 1
    FROM Patient AS p
    WHERE p.ID = l.ID AND p.SEX = 'F'
  )
  AND EXISTS (
    SELECT 1
    FROM Examination AS e
    WHERE e.ID = l.ID
      AND ',' || REPLACE(REPLACE(REPLACE(UPPER(e.Diagnosis), ' ', ''), '+', ','), '->', ',') || ',' LIKE '%,SLE,%'
  );
