SELECT school.AdmFName1 AS first_name, school.AdmLName1 AS last_name
FROM main.schools AS ref
JOIN main.schools AS school ON school.County = ref.County
JOIN main.satscores AS sat ON sat.cds = school.CDSCode
WHERE ref.CDSCode = '01611430131177'
  AND sat.rtype = 'S'
  AND sat.NumGE1500 IS NOT NULL
ORDER BY sat.NumGE1500 DESC, school.CDSCode ASC
LIMIT 1
