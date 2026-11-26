SELECT c.colour AS hair_colour
FROM main.superhero AS h
JOIN main.race AS r ON r.id = h.race_id
LEFT JOIN main.colour AS c ON c.id = h.hair_colour_id
WHERE r.race = 'Human' AND h.height_cm > 0
ORDER BY h.height_cm
LIMIT 1;
