WITH ranked_schools AS (
    SELECT s.School AS school_name,
           s.District AS district,
           s.County AS county,
           sa.AvgScrWrite AS average_sat_writing_score,
           DENSE_RANK() OVER (ORDER BY sa.AvgScrWrite DESC) AS score_rank
    FROM schools AS s
    JOIN satscores AS sa ON sa.cds = s.CDSCode
    WHERE s.StatusType = 'Closed'
      AND sa.rtype = 'S'
      AND sa.AvgScrWrite IS NOT NULL
)
SELECT school_name, district, county, average_sat_writing_score
FROM ranked_schools
WHERE score_rank = 2
ORDER BY school_name, district, county;
