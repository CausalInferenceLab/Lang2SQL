SELECT AVG(m.height_cm) AS average_height_cm
FROM superhero AS m
JOIN publisher AS mp ON mp.id = m.publisher_id
WHERE mp.publisher_name = 'Marvel Comics'
  AND m.height_cm > 0
  AND EXISTS (
    SELECT 1
    FROM hero_power AS mhp
    JOIN superpower AS sp ON sp.id = mhp.power_id
    WHERE mhp.hero_id = m.id
      AND NOT EXISTS (
        SELECT 1
        FROM hero_power AS dhp
        JOIN superhero AS d ON d.id = dhp.hero_id
        JOIN publisher AS dp ON dp.id = d.publisher_id
        WHERE dp.publisher_name = 'DC Comics'
          AND dhp.power_id = sp.id
      )
  );
