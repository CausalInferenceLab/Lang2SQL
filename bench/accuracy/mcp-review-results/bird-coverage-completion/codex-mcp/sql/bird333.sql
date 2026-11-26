SELECT h.superhero_name
FROM superhero AS h
JOIN publisher AS p ON p.id = h.publisher_id
WHERE p.publisher_name = 'Marvel Comics'
  AND h.id IN (
      SELECT hp.hero_id
      FROM hero_power AS hp
      JOIN superpower AS sp ON sp.id = hp.power_id
      WHERE sp.power_name = 'Super Strength'
  )
ORDER BY h.superhero_name
LIMIT 1000
