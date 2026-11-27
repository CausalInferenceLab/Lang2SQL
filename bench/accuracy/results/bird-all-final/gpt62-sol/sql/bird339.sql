SELECT s.superhero_name, p.publisher_name
FROM superhero AS s
JOIN colour AS c ON c.id = s.eye_colour_id
LEFT JOIN publisher AS p ON p.id = s.publisher_id
WHERE s.eye_colour_id = s.hair_colour_id
  AND s.eye_colour_id = s.skin_colour_id
  AND c.colour <> 'No Colour'
ORDER BY s.superhero_name, p.publisher_name;
