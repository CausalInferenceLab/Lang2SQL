SELECT COUNT(DISTINCT s.CDSCode) AS school_count
FROM schools AS s
WHERE s.StatusType = 'Active'
  AND s.EILName = 'High School'
  AND NOT EXISTS (
    SELECT 1
    FROM satscores AS sat
    WHERE sat.cds = s.CDSCode
      AND sat.NumTstTakr <> 0
  );
