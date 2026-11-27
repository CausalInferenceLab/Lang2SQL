SELECT COUNT(DISTINCT p.id) AS superpower_count
FROM superhero AS s
JOIN gender AS g ON g.id = s.gender_id
JOIN hero_power AS hp ON hp.hero_id = s.id
JOIN superpower AS p ON p.id = hp.power_id
WHERE g.gender = 'Male'
  AND s.weight_kg > 0
  AND s.weight_kg < 70;
