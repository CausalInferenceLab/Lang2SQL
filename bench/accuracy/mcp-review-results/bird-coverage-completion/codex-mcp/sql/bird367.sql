SELECT
    ROUND(AVG(NULLIF(s.height_cm, 0)), 1) AS average_height_cm,
    ROUND(AVG(NULLIF(s.weight_kg, 0)), 1) AS average_weight_kg
FROM superhero AS s
JOIN publisher AS p ON p.id = s.publisher_id
WHERE p.publisher_name = 'George Lucas'
LIMIT 1
