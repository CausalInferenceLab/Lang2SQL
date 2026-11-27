SELECT 100.0 * COUNT(CASE WHEN health = 'Good' THEN 1 END) / COUNT(*) AS good_health_percentage
FROM trees
WHERE boroname = 'Bronx';
