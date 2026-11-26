WITH ranked_schools AS (
    SELECT s.School, s.District, s.County, sat.AvgScrWrite,
           DENSE_RANK() OVER (ORDER BY sat.AvgScrWrite DESC) AS score_rank
    FROM schools AS s
    JOIN satscores AS sat ON sat.cds = s.CDSCode
    WHERE s.StatusType = 'Closed'
      AND s.State = 'CA'
      AND sat.rtype = 'S'
      AND sat.AvgScrWrite IS NOT NULL
)
SELECT School, District, County, AvgScrWrite
FROM ranked_schools
WHERE score_rank = 2
ORDER BY School, District, County
LIMIT 1000
