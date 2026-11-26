SELECT c.colour
FROM superhero AS s
JOIN race AS r ON r.id = s.race_id
LEFT JOIN colour AS c ON c.id = s.hair_colour_id
WHERE r.race = 'Human'
  AND s.height_cm > 0
ORDER BY s.height_cm, s.id
LIMIT 1;
