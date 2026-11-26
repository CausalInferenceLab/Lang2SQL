SELECT a.Street, a.City, a.Zip, a.State
FROM satscores AS s
JOIN schools AS a ON a.CDSCode = s.cds
LEFT JOIN frpm AS f ON f.CDSCode = s.cds
WHERE s.rtype = 'S'
  AND s.NumTstTakr >= 700
  AND s.NumGE1500 IS NOT NULL
ORDER BY CAST(s.NumGE1500 AS REAL) / s.NumTstTakr ASC,
         CAST(f."School Code" AS INTEGER) ASC NULLS LAST
LIMIT 1;
