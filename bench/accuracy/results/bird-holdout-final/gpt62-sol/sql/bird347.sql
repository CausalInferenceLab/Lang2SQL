WITH power_counts AS (
    SELECT sp.power_name, COUNT(DISTINCT h.id) AS hero_count
    FROM superhero AS h
    JOIN publisher AS p ON p.id = h.publisher_id
    JOIN hero_power AS hp ON hp.hero_id = h.id
    JOIN superpower AS sp ON sp.id = hp.power_id
    WHERE p.publisher_name = 'Marvel Comics'
      AND h.full_name IS NOT NULL
      AND TRIM(h.full_name) NOT IN ('', '-')
    GROUP BY sp.id, sp.power_name
)
SELECT power_name, hero_count
FROM power_counts
WHERE hero_count = (SELECT MAX(hero_count) FROM power_counts)
ORDER BY power_name;
