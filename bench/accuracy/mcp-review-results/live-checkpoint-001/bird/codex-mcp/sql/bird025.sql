SELECT ROUND(AVG(COALESCE(t.NumTstTakr, 0)), 2) AS average_sat_test_takers
FROM main.schools AS s
LEFT JOIN main.satscores AS t
    ON t.cds = s.CDSCode AND t.rtype = 'S'
WHERE s.County = 'Los Angeles'
  AND s.School IS NOT NULL
  AND s.OpenDate >= '2007-01-01'
  AND s.OpenDate < '2008-01-01'
  AND s.DOCType IN (
      'Elementary School District',
      'High School District',
      'Unified School District',
      'County Office of Education (COE)'
  )
LIMIT 1
