SELECT ROUND(100.0 * COUNT(CASE WHEN c.colour = 'Blue' THEN 1 END) / COUNT(*), 2) AS percentage
FROM superhero AS s
JOIN gender AS g ON g.id = s.gender_id
LEFT JOIN colour AS c ON c.id = s.skin_colour_id
WHERE g.gender = 'Female';
