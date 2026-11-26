WITH reference_hero AS (
    SELECT id
    FROM main.superhero
    WHERE superhero_name = '3-D Man'
),
reference_powers AS (
    SELECT power_id
    FROM main.hero_power
    WHERE hero_id IN (SELECT id FROM reference_hero)
)
SELECT ROUND(AVG(h.weight_kg), 2) AS average_weight_kg
FROM main.superhero AS h
WHERE h.id NOT IN (SELECT id FROM reference_hero)
  AND h.weight_kg <> 0
  AND h.id IN (
      SELECT hero_id
      FROM main.hero_power
      WHERE power_id IN (SELECT power_id FROM reference_powers)
  )
LIMIT 1
