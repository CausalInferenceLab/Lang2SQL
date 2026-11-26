WITH ranked_heroes AS (
    SELECT id, RANK() OVER (ORDER BY height_cm DESC) AS height_rank
    FROM main.superhero
)
SELECT COUNT(DISTINCT p.power_name) AS distinct_superpowers
FROM ranked_heroes AS h
JOIN main.hero_power AS hp ON hp.hero_id = h.id
JOIN main.superpower AS p ON p.id = hp.power_id
WHERE h.height_rank <= 10
LIMIT 1000
