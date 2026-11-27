SELECT CAST(ROUND(AVG(dc.weight_kg)) AS INTEGER) AS average_weight_kg
FROM superhero AS dc
JOIN publisher AS dp ON dp.id = dc.publisher_id
WHERE dp.publisher_name = 'DC Comics'
  AND dc.weight_kg IS NOT NULL
  AND EXISTS (
    SELECT 1
    FROM superhero AS marvel
    JOIN publisher AS mp ON mp.id = marvel.publisher_id
    WHERE mp.publisher_name = 'Marvel Comics'
      AND marvel.eye_colour_id = dc.eye_colour_id
  );
