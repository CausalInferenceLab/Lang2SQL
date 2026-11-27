SELECT s.superhero_name
FROM superhero AS s
JOIN colour AS c ON c.id = s.eye_colour_id
WHERE s.height_cm BETWEEN 170 AND 190
  AND c.colour = 'No Colour';
