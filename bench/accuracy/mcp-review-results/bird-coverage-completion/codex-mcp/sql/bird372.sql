SELECT COUNT(DISTINCT hp.power_id) AS superpower_count
FROM main.hero_power AS hp
JOIN main.superhero AS h ON h.id = hp.hero_id
JOIN main.gender AS g ON g.id = h.gender_id
WHERE g.gender = 'Male'
  AND h.weight_kg > 0
  AND h.weight_kg < 70
LIMIT 1000
