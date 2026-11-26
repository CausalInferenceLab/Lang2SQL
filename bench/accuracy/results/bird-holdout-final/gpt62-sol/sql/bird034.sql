SELECT ROUND(100.0 * COUNT(CASE WHEN s.Virtual IN ('F', 'P') THEN 1 END) / COUNT(*), 1) AS virtual_percentage
FROM schools AS s
JOIN satscores AS t ON t.cds = s.CDSCode
WHERE s.State = 'CA'
  AND t.rtype = 'S'
  AND t.AvgScrMath > 400;
