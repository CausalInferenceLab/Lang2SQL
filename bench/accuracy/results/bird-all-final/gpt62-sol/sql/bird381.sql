SELECT COUNT(CASE WHEN c.colour = 'No Colour' THEN 1 END)
     - COUNT(CASE WHEN c.colour = 'Blue' THEN 1 END) AS eye_count_difference
FROM superhero AS s
JOIN colour AS c ON c.id = s.eye_colour_id
WHERE s.weight_kg IS NULL;
