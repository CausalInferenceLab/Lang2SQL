SELECT ROUND(AVG(COALESCE(t.NumTstTakr, 0)), 2) AS avg_sat_test_takers_per_school
FROM schools AS s
LEFT JOIN satscores AS t ON t.cds = s.CDSCode
WHERE s.CharterNum = '0756'
  AND s.MailCity = 'San Diego';
