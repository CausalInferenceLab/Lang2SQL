SELECT ROUND(AVG(l.ALB), 4) AS average_alb
FROM Laboratory AS l
JOIN Patient AS p ON p.ID = l.ID
WHERE l.PLT > 400
  AND p.SEX = 'F'
  AND p.Diagnosis LIKE '%SLE%';
