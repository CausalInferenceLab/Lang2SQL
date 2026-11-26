SELECT COUNT(*) AS school_count
FROM main.schools AS s
LEFT JOIN main.satscores AS sat ON sat.cds = s.CDSCode
WHERE s.State = 'CA'
  AND s.StatusType = 'Active'
  AND s.EILName = 'High School'
  AND (sat.cds IS NULL OR sat.NumTstTakr = 0)
LIMIT 1
