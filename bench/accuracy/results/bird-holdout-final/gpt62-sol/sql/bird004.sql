SELECT sc.MailStreet, sc.School
FROM satscores AS s
JOIN schools AS sc ON sc.CDSCode = s.cds
WHERE s.rtype = 'S'
  AND s.AvgScrMath IS NOT NULL
ORDER BY s.AvgScrMath DESC, s.cds
LIMIT 1 OFFSET 6;
