WITH eligible_schools AS (
    SELECT s.School, s.AdmEmail1, sat.NumGE1500
    FROM schools AS s
    JOIN satscores AS sat ON sat.cds = s.CDSCode
    WHERE s.DOCType = 'State Board of Education'
      AND sat.rtype = 'S'
)
SELECT School, AdmEmail1
FROM eligible_schools
WHERE NumGE1500 = (SELECT MAX(NumGE1500) FROM eligible_schools);
