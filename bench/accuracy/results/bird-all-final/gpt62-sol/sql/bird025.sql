SELECT ROUND(AVG(COALESCE(sat.NumTstTakr, 0)), 2) AS avg_sat_test_takers
FROM schools AS s
LEFT JOIN satscores AS sat
  ON sat.cds = s.CDSCode AND sat.rtype = 'S'
WHERE s.County = 'Los Angeles'
  AND s.OpenDate >= '2007-01-01'
  AND s.OpenDate < '2008-01-01'
  AND s.School IS NOT NULL
  AND s.DOCType IN (
    'County Office of Education (COE)',
    'Elementary School District',
    'High School District',
    'Unified School District'
  );
