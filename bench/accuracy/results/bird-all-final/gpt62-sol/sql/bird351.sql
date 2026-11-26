WITH ranked_heroes AS (
    SELECT id, RANK() OVER (ORDER BY height_cm DESC) AS height_rank
    FROM superhero
)
SELECT COUNT(DISTINCT sp.power_name) AS distinct_superpower_count
FROM ranked_heroes AS rh
LEFT JOIN hero_power AS hp ON hp.hero_id = rh.id
LEFT JOIN superpower AS sp ON sp.id = hp.power_id
WHERE rh.height_rank <= 10;
