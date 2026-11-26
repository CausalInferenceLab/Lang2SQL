SELECT
    c.colour AS eye_colour,
    COUNT(*) AS hero_count,
    RANK() OVER (ORDER BY COUNT(*) DESC) AS colour_rank
FROM main.superhero AS s
JOIN main.publisher AS p ON p.id = s.publisher_id
JOIN main.colour AS c ON c.id = s.eye_colour_id
WHERE p.publisher_name = 'Marvel Comics'
  AND c.colour <> 'No Colour'
GROUP BY c.colour
ORDER BY hero_count DESC, eye_colour ASC
LIMIT 1000
