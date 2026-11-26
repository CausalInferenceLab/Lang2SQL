SELECT ROUND(AVG(h.weight_kg), 2) AS average_weight_kg
FROM superhero AS h
JOIN publisher AS p ON p.id = h.publisher_id
WHERE p.publisher_name = 'George Lucas'
  AND h.weight_kg IS NOT NULL
  AND EXISTS (
    SELECT 1
    FROM hero_power AS hp
    JOIN superpower AS sp ON sp.id = hp.power_id
    WHERE hp.hero_id = h.id
  );
