SELECT COUNT(*) AS qualifying_schools
FROM schools AS s
JOIN satscores AS sat ON sat.cds = s.CDSCode
WHERE s.MailCity = 'San Francisco'
  AND sat.rtype = 'S'
  AND s.DOCType IN (
      'Unified School District',
      'High School District',
      'Elementary School District',
      'County Office of Education (COE)'
  )
  AND sat.AvgScrRead + sat.AvgScrMath + sat.AvgScrWrite >= 1500
LIMIT 1
