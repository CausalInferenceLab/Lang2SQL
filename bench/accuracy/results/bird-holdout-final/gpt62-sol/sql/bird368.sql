SELECT ROUND(AVG(h.height_cm), 1) AS average_height_cm
FROM superhero AS h
WHERE h.height_cm > 0
  AND EXISTS (
    SELECT 1
    FROM hero_power AS hp
    JOIN hero_power AS target_hp ON target_hp.power_id = hp.power_id
    JOIN superhero AS target ON target.id = target_hp.hero_id
    WHERE hp.hero_id = h.id
      AND target.superhero_name = '3-D Man'
      AND h.id <> target.id
  );
