SELECT ROUND(AVG(l.ALB), 4) AS average_alb
FROM main.Laboratory AS l
JOIN main.Patient AS p ON p.ID = l.ID
WHERE l.PLT > 400
  AND p.SEX = 'F'
  AND p.Diagnosis LIKE '%SLE%'
  AND p.Diagnosis NOT LIKE '%SLE susp%'
LIMIT 1
