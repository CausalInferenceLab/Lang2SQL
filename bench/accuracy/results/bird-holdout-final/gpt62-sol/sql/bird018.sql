SELECT COUNT(DISTINCT sc.CDSCode) AS qualifying_school_count
FROM schools AS sc
JOIN satscores AS sat ON sat.cds = sc.CDSCode
WHERE sc.MailCity = 'San Francisco'
  AND sc.DOCType IN (
    'Unified School District',
    'High School District',
    'Elementary School District',
    'County Office of Education (COE)'
  )
  AND sat.rtype = 'S'
  AND sat.AvgScrRead + sat.AvgScrMath + sat.AvgScrWrite >= 1500;
