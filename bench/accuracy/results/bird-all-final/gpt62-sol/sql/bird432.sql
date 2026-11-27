SELECT COUNT(DISTINCT p.ID) AS patient_count
FROM Patient AS p
JOIN Examination AS e ON e.ID = p.ID
WHERE p.SEX = 'F'
  AND p.Admission LIKE '+%'
  AND strftime('%Y', p.Birthday) BETWEEN '1945' AND '1989'
  AND e."ANA Pattern" = 'P';
