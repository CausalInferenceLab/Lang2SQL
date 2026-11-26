WITH ranked_schools AS (
    SELECT s.School, s.District, s.County, t.AvgScrWrite,
           DENSE_RANK() OVER (ORDER BY t.AvgScrWrite DESC) AS score_rank
    FROM main.schools AS s
    JOIN main.satscores AS t ON t.cds = s.CDSCode
    WHERE s.StatusType = 'Closed'
      AND s.State = 'CA'
      AND t.rtype = 'S'
      AND t.AvgScrWrite IS NOT NULL
)
SELECT School, District, County, AvgScrWrite
FROM ranked_schools
WHERE score_rank = 2
ORDER BY School, District, County
LIMIT -1
