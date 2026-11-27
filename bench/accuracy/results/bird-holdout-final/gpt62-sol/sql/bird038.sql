WITH virtual_schools AS (
    SELECT CDSCode, County, School
    FROM schools
    WHERE Virtual = 'F'
), county_counts AS (
    SELECT County,
           COUNT(*) AS school_count,
           RANK() OVER (ORDER BY COUNT(*) DESC) AS county_rank
    FROM virtual_schools
    GROUP BY County
), ranked_reading AS (
    SELECT v.CDSCode, v.County, v.School,
           RANK() OVER (
               PARTITION BY v.County
               ORDER BY t.AvgScrRead DESC
           ) AS reading_rank
    FROM virtual_schools AS v
    JOIN satscores AS t ON t.cds = v.CDSCode
    WHERE t.AvgScrRead IS NOT NULL
)
SELECT c.County AS county_name,
       c.school_count AS exclusively_virtual_school_count,
       CASE WHEN r.CDSCode IS NULL THEN 'None' ELSE r.School END AS school_name
FROM county_counts AS c
LEFT JOIN ranked_reading AS r
    ON r.County = c.County AND r.reading_rank = 1
WHERE c.county_rank <= 5
ORDER BY c.school_count DESC, c.County, school_name;
