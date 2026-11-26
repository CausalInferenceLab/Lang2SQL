WITH eligible AS (
    SELECT s.School, s.MailStreet, t.AvgScrRead
    FROM main.schools AS s
    JOIN main.satscores AS t ON t.cds = s.CDSCode
    WHERE s.DOCType = 'State Board of Education'
      AND t.rtype = 'S'
      AND t.AvgScrRead IS NOT NULL
)
SELECT School, MailStreet
FROM eligible
WHERE AvgScrRead = (SELECT MAX(AvgScrRead) FROM eligible)
ORDER BY School, MailStreet
LIMIT -1
