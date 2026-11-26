SELECT COUNT(*) FILTER (WHERE c.colour = 'No Colour')
     - COUNT(*) FILTER (WHERE c.colour = 'Blue') AS count_difference
FROM main.superhero AS s
JOIN main.colour AS c ON c.id = s.eye_colour_id
WHERE s.weight_kg IS NULL OR s.weight_kg = 0
LIMIT 1
