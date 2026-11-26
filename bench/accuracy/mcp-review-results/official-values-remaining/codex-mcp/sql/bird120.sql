SELECT
    100.0 * (
        COUNT(*) FILTER (WHERE Date >= '2010-01-01' AND Date < '2011-01-01')
        - COUNT(*) FILTER (WHERE Date >= '2011-01-01' AND Date < '2012-01-01')
    ) / NULLIF(COUNT(*), 0) AS percentage_point_difference
FROM main.badges
WHERE Name = 'Student'
LIMIT 1
