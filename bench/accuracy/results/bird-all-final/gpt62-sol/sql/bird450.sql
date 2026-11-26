SELECT p.SEX AS sex, COUNT(DISTINCT p.ID) AS patient_count
FROM Patient AS p
WHERE p.SEX IN ('F', 'M')
  AND NOT EXISTS (
    SELECT 1
    FROM Examination AS e
    WHERE e.ID = p.ID
  )
GROUP BY p.SEX
ORDER BY p.SEX;
