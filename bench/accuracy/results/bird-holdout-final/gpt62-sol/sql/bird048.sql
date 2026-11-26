SELECT s.County AS county_name,
       COALESCE(SUM(t.NumTstTakr), 0) AS total_sat_test_takers
FROM schools AS s
JOIN frpm AS f ON f.CDSCode = s.CDSCode
LEFT JOIN satscores AS t ON t.cds = s.CDSCode AND t.rtype = 'S'
WHERE f."NSLP Provision Status" = 'Provision 2'
  AND s.Magnet = 1
GROUP BY s.County;
