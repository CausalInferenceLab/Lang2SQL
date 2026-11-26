WITH strength_heroes AS (
    SELECT DISTINCT hp.hero_id
    FROM main.hero_power AS hp
    JOIN main.superpower AS p ON p.id = hp.power_id
    WHERE p.power_name = 'Super Strength'
),
exclusive_powers AS (
    SELECT hp.power_id
    FROM main.hero_power AS hp
    LEFT JOIN strength_heroes AS sh ON sh.hero_id = hp.hero_id
    WHERE hp.hero_id IS NOT NULL AND hp.power_id IS NOT NULL
    GROUP BY hp.power_id
    HAVING COUNT(sh.hero_id) = COUNT(*)
)
SELECT COUNT(*) AS power_count
FROM exclusive_powers
LIMIT 1000
