SELECT sat.AvgScrMath, sat.NumTstTakr
FROM satscores AS sat
JOIN schools AS s ON s.CDSCode = sat.cds
WHERE s.CDSCode = '01611190130609'
  AND s.Charter = 1
  AND s.FundingType = 'Directly funded'
LIMIT 1
