WITH cryokinesis_holders AS (
    SELECT hp.hero_id
    FROM main.hero_power AS hp
    JOIN main.superpower AS sp ON sp.id = hp.power_id
    WHERE sp.power_name = 'Cryokinesis'
),
holder_powers AS (
    SELECT hp.power_id
    FROM main.hero_power AS hp
    WHERE hp.hero_id IN (SELECT hero_id FROM cryokinesis_holders)
),
hero_power_counts AS (
    SELECT hp.hero_id, COUNT(DISTINCT hp.power_id) AS power_count
    FROM main.hero_power AS hp
    GROUP BY hp.hero_id
    HAVING MAX(hp.power_id IN (SELECT power_id FROM holder_powers)) = 1
)
SELECT ROUND(AVG(power_count), 1) AS average_superpowers
FROM hero_power_counts
LIMIT 1000
