WITH hero_powers AS (
    SELECT h.id AS hero_id, hp.power_id
    FROM superhero AS h
    JOIN hero_power AS hp ON hp.hero_id = h.id
), strength_heroes AS (
    SELECT DISTINCT hp.hero_id
    FROM hero_powers AS hp
    JOIN superpower AS p ON p.id = hp.power_id
    WHERE p.power_name = 'Super Strength'
)
SELECT COUNT(*) AS power_count
FROM superpower AS p
WHERE EXISTS (
    SELECT 1
    FROM hero_powers AS hp
    JOIN strength_heroes AS sh ON sh.hero_id = hp.hero_id
    WHERE hp.power_id = p.id
)
AND NOT EXISTS (
    SELECT 1
    FROM hero_powers AS hp
    LEFT JOIN strength_heroes AS sh ON sh.hero_id = hp.hero_id
    WHERE hp.power_id = p.id AND sh.hero_id IS NULL
);
