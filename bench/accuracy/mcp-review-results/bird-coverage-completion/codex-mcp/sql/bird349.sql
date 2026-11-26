WITH qualifying_heroes AS (
    SELECT h.id, h.gender_id
    FROM superhero AS h
    JOIN colour AS hair ON hair.id = h.hair_colour_id
    JOIN hero_power AS hp ON hp.hero_id = h.id
    WHERE hair.colour = 'Blue'
    GROUP BY h.id, h.gender_id
    HAVING COUNT(DISTINCT hp.power_id) >= 10
)
SELECT g.gender, COUNT(q.id) AS superhero_count
FROM gender AS g
LEFT JOIN qualifying_heroes AS q ON q.gender_id = g.id
WHERE g.gender IN ('Male', 'Female')
GROUP BY g.gender
ORDER BY g.gender
LIMIT 1000
