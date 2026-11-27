SELECT AVG(h.weight_kg) AS average_weight_kg
FROM superhero AS h
JOIN race AS r ON r.id = h.race_id
WHERE h.weight_kg IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM superhero AS f
    JOIN gender AS g ON g.id = f.gender_id
    WHERE f.race_id = r.id
      AND g.gender = 'Female'
  );
