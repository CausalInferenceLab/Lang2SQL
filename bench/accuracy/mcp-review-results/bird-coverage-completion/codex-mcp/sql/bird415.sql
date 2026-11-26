SELECT COUNT(CASE WHEN p.SEX = 'F' THEN 1 END)
     - COUNT(CASE WHEN p.SEX = 'M' THEN 1 END) AS women_minus_men
FROM Patient AS p
WHERE p.Diagnosis = 'RA'
  AND EXISTS (
      SELECT 1
      FROM Laboratory AS l
      WHERE l.ID = p.ID
        AND l.UN < 30
  )
LIMIT 1000
