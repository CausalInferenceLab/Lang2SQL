SELECT
    COUNT(CASE WHEN c.colour = 'No Colour' THEN 1 END)
    - COUNT(CASE WHEN c.colour = 'Blue' THEN 1 END) AS difference
FROM main.superhero AS s
JOIN main.colour AS c ON c.id = s.eye_colour_id
WHERE s.weight_kg IS NULL
LIMIT 1
