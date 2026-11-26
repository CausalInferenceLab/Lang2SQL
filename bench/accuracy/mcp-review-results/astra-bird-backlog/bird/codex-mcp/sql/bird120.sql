SELECT
    100.0 * (
        COUNT(*) FILTER (WHERE strftime('%Y', Date) = '2010')
        - COUNT(*) FILTER (WHERE strftime('%Y', Date) = '2011')
    ) / NULLIF(COUNT(*), 0) AS difference_percentage_points
FROM main.badges
WHERE Name = 'Student'
LIMIT 1
