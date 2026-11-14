SELECT 
  100.0 * SUM(CASE WHEN health = 'Good' THEN 1 ELSE 0 END) / COUNT(*) AS percent_good
FROM trees
WHERE boroname = 'Bronx'
