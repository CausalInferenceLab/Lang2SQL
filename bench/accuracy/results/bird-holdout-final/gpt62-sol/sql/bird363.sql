SELECT ROUND(AVG(s.weight_kg), 2) AS average_weight_kg
FROM superhero AS s
JOIN gender AS g ON g.id = s.gender_id
WHERE g.gender = 'Female'
  AND s.weight_kg IS NOT NULL;
