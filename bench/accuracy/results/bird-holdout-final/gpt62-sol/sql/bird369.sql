SELECT CAST(ROUND(AVG(s.weight_kg)) AS INTEGER) AS average_weight_kg
FROM superhero AS s
JOIN alignment AS a ON a.id = s.alignment_id
JOIN colour AS c ON c.id = s.skin_colour_id
WHERE a.alignment = 'Bad' COLLATE NOCASE
  AND c.colour = 'Green' COLLATE NOCASE
  AND s.weight_kg > 0
  AND EXISTS (
    SELECT 1
    FROM hero_power AS hp
    JOIN superpower AS p ON p.id = hp.power_id
    WHERE hp.hero_id = s.id
      AND p.power_name = 'Flight' COLLATE NOCASE
  )
