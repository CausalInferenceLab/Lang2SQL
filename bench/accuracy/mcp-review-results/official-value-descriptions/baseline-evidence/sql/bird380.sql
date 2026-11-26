WITH shared_power_heroes AS (
    SELECT candidate.hero_id
    FROM main.superhero AS target
    JOIN main.hero_power AS owned ON owned.hero_id = target.id
    JOIN main.hero_power AS candidate ON candidate.power_id = owned.power_id
    WHERE target.superhero_name = '3-D Man'
      AND candidate.hero_id <> target.id
)
SELECT ROUND(AVG(hero.weight_kg), 2) AS average_weight_kg
FROM main.superhero AS hero
WHERE hero.weight_kg IS NOT NULL
  AND hero.id IN (SELECT hero_id FROM shared_power_heroes)
LIMIT 1
