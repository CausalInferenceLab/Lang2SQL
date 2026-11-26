SELECT ROUND(AVG(h.weight_kg), 2) AS average_weight_kg
FROM superhero AS h
WHERE h.weight_kg > 0
  AND EXISTS (
    SELECT 1
    FROM superhero AS target
    JOIN hero_power AS tp ON tp.hero_id = target.id
    JOIN hero_power AS hp ON hp.power_id = tp.power_id
    WHERE target.superhero_name = '3-D Man'
      AND hp.hero_id = h.id
      AND h.id <> target.id
  );
