SELECT 100.0 * COUNT(CASE WHEN health = 'Good' THEN 1 END) / COUNT(*) AS percentage_good
FROM trees
WHERE boroname = 'Bronx';
