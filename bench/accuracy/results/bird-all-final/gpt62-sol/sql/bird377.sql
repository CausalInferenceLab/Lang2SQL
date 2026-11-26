SELECT eye.colour AS eye_colour,
       COUNT(CASE WHEN hair.colour <> 'Brown' THEN 1 END) AS hero_count
FROM superhero AS h
JOIN colour AS eye ON eye.id = h.eye_colour_id
LEFT JOIN colour AS hair ON hair.id = h.hair_colour_id
GROUP BY eye.colour
HAVING COUNT(CASE WHEN hair.colour = 'Brown' THEN 1 END) > 0
ORDER BY hero_count DESC, eye_colour ASC;
