SELECT ROUND(
    100.0 * COUNT(*) FILTER (WHERE c.colour = 'Blue') / NULLIF(COUNT(*), 0),
    2
) AS blue_skin_percentage
FROM superhero AS s
JOIN gender AS g ON g.id = s.gender_id
LEFT JOIN colour AS c ON c.id = s.skin_colour_id
WHERE g.gender = 'Female'
LIMIT 1
