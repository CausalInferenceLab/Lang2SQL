SELECT ROUND(AVG(s.weight_kg), 1) AS average_weight_kg
FROM superhero AS s
JOIN colour AS c ON c.id = s.skin_colour_id
WHERE LOWER(c.colour) = 'green'
  AND s.weight_kg > 0;
