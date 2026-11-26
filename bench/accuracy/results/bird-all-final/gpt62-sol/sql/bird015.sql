WITH eligible AS (
    SELECT s.School, s.Phone, a.NumGE1500
    FROM schools AS s
    JOIN satscores AS a ON a.cds = s.CDSCode
    WHERE s.DOCType = 'Statewide Benefit Charter'
      AND a.rtype = 'S'
)
SELECT School, Phone
FROM eligible
WHERE NumGE1500 = (SELECT MAX(NumGE1500) FROM eligible);
