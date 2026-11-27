SELECT 100.0 * COUNT(CASE WHEN health = 'Good' THEN 1 END) / NULLIF(COUNT(*), 0) AS percentage_good
FROM trees
WHERE boroname = 'Bronx';
