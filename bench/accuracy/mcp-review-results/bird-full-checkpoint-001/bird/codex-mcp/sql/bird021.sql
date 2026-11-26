SELECT ROUND(AVG(COALESCE(f."FRPM Count (Ages 5-17)", 0)), 2) AS average_eligible_students
FROM schools AS s
LEFT JOIN frpm AS f
  ON f.CDSCode = s.CDSCode
 AND f."Academic Year" = '2014-2015'
WHERE s.SOCType = 'Youth Authority Facilities'
LIMIT 1
