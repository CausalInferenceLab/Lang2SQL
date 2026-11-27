SELECT h.superhero_name
FROM superhero AS h
JOIN publisher AS p ON p.id = h.publisher_id
WHERE p.publisher_name = 'Marvel Comics'
  AND EXISTS (
    SELECT 1
    FROM hero_power AS hp
    JOIN superpower AS sp ON sp.id = hp.power_id
    WHERE hp.hero_id = h.id
      AND sp.power_name = 'Super Strength'
  );
