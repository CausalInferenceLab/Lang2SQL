WITH cryokinesis_holder_powers AS (
    SELECT DISTINCT hp.power_id
    FROM hero_power AS hp
    WHERE hp.hero_id IN (
        SELECT ch.hero_id
        FROM hero_power AS ch
        JOIN superpower AS sp ON sp.id = ch.power_id
        WHERE sp.power_name = 'Cryokinesis'
    )
), matching_heroes AS (
    SELECT DISTINCT hp.hero_id
    FROM hero_power AS hp
    JOIN cryokinesis_holder_powers AS cp ON cp.power_id = hp.power_id
), hero_power_counts AS (
    SELECT hp.hero_id, COUNT(DISTINCT hp.power_id) AS power_count
    FROM matching_heroes AS mh
    JOIN superhero AS h ON h.id = mh.hero_id
    JOIN hero_power AS hp ON hp.hero_id = h.id
    GROUP BY hp.hero_id
)
SELECT ROUND(AVG(power_count), 1) AS average_superpowers
FROM hero_power_counts;
