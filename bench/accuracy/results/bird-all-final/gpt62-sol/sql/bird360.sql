SELECT ROUND(AVG(s.weight_kg), 1) AS average_weight_kg,
       COUNT(s.weight_kg) AS superhero_count
FROM superhero AS s
JOIN publisher AS p ON p.id = s.publisher_id
WHERE p.publisher_name = 'DC Comics';
