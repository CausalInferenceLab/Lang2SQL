WITH eligible AS (
    SELECT s.School, s.MailStreet, a.AvgScrRead
    FROM schools AS s
    JOIN satscores AS a ON a.cds = s.CDSCode
    WHERE s.DOCType = 'State Board of Education'
      AND a.rtype = 'S'
)
SELECT School, MailStreet
FROM eligible
WHERE AvgScrRead = (SELECT MAX(AvgScrRead) FROM eligible);
