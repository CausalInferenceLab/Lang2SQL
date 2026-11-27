WITH powers_per_hero AS (
  SELECT h.id, COUNT(DISTINCT hp.power_id) AS power_count
  FROM superhero AS h
  JOIN race AS r ON r.id = h.race_id
  JOIN gender AS g ON g.id = h.gender_id
  LEFT JOIN hero_power AS hp ON hp.hero_id = h.id
  WHERE r.race = 'God / Eternal' AND g.gender = 'Male'
  GROUP BY h.id
)
SELECT ROUND(AVG(power_count), 2) AS average_superpowers
FROM powers_per_hero;
