SELECT p.SEX, COUNT(DISTINCT p.ID) AS patient_count
FROM Patient AS p
JOIN Laboratory AS l ON l.ID = p.ID
WHERE l."Date" >= '1994-01-01'
  AND l."Date" < '1995-01-01'
  AND l.GOT < 60
  AND p.SEX IN ('F', 'M')
GROUP BY p.SEX
ORDER BY p.SEX
LIMIT 1000
