WITH hulk_strength AS (
    SELECT ha.attribute_value
    FROM superhero AS h
    JOIN hero_attribute AS ha ON ha.hero_id = h.id
    JOIN attribute AS a ON a.id = ha.attribute_id
    WHERE h.superhero_name = 'Hulk'
      AND a.attribute_name = 'Strength'
)
SELECT CAST(ROUND(AVG(s.weight_kg)) AS INTEGER) AS average_weight_kg
FROM superhero AS s
WHERE s.weight_kg > 0
  AND EXISTS (
      SELECT 1
      FROM hero_attribute AS ha
      JOIN attribute AS a ON a.id = ha.attribute_id
      WHERE ha.hero_id = s.id
        AND a.attribute_name = 'Strength'
        AND ha.attribute_value IN (SELECT attribute_value FROM hulk_strength)
  );
