WITH power_counts AS (
    SELECT COUNT(DISTINCT p.power_name) AS power_count
    FROM main.superhero AS h
    LEFT JOIN main.hero_power AS hp ON hp.hero_id = h.id
    LEFT JOIN main.superpower AS p ON p.id = hp.power_id
    WHERE h.weight_kg = (
        SELECT MIN(weight_kg)
        FROM main.superhero
        WHERE weight_kg <> 0
    )
    GROUP BY h.id
)
SELECT MAX(power_count) AS max_superpowers
FROM power_counts
LIMIT 1000
