SELECT s.MailStrAbr, s.School
FROM main.satscores AS t
JOIN main.schools AS s ON s.CDSCode = t.cds
WHERE t.rtype = 'S'
  AND t.AvgScrMath IS NOT NULL
ORDER BY t.AvgScrMath DESC
LIMIT 1 OFFSET 6;
