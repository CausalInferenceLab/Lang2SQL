SELECT CAST(ROUND(AVG(s.weight_kg)) AS INTEGER) AS average_weight_kg
FROM superhero AS s
JOIN colour AS c ON c.id = s.skin_colour_id
JOIN alignment AS a ON a.id = s.alignment_id
WHERE c.colour = 'Green'
  AND a.alignment = 'Bad'
  AND s.weight_kg > 0
  AND s.id IN (
      SELECT hp.hero_id
      FROM hero_power AS hp
      JOIN superpower AS p ON p.id = hp.power_id
      WHERE p.power_name = 'Flight'
  )
LIMIT 1
