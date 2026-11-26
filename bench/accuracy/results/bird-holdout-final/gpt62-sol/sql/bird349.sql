WITH qualified_heroes AS (
    SELECT h.id, h.gender_id
    FROM superhero AS h
    JOIN colour AS c ON c.id = h.hair_colour_id
    JOIN hero_power AS hp ON hp.hero_id = h.id
    WHERE LOWER(c.colour) = 'blue'
    GROUP BY h.id, h.gender_id
    HAVING COUNT(DISTINCT hp.power_id) >= 10
)
SELECT g.gender, COUNT(q.id) AS superhero_count
FROM gender AS g
LEFT JOIN qualified_heroes AS q ON q.gender_id = g.id
WHERE LOWER(g.gender) IN ('male', 'female')
GROUP BY g.gender
ORDER BY g.gender;
