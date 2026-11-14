SELECT ROUND(100.0 * SUM(CASE WHEN health = 'Good' THEN 1 ELSE 0 END) / COUNT(*), 2) AS percentage_good_health
FROM trees
WHERE borocode = 2;
