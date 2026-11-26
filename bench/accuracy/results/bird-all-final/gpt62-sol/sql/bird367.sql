SELECT
    ROUND(AVG(CASE WHEN s.height_cm > 0 THEN s.height_cm END), 1) AS average_height_cm,
    ROUND(AVG(CASE WHEN s.weight_kg > 0 THEN s.weight_kg END), 1) AS average_weight_kg
FROM superhero AS s
JOIN publisher AS p ON p.id = s.publisher_id
WHERE p.publisher_name = 'George Lucas';
