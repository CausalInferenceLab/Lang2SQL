SELECT 100.0 * (
    COUNT(CASE WHEN strftime('%Y', Date) = '2010' THEN 1 END)
    - COUNT(CASE WHEN strftime('%Y', Date) = '2011' THEN 1 END)
) / NULLIF(COUNT(*), 0) AS difference_percentage_points
FROM badges
WHERE Name = 'Student';
