WITH lightest_weight AS (
    SELECT MIN(NULLIF(weight_kg, 0)) AS weight_kg
    FROM superhero
), hero_power_counts AS (
    SELECT h.id, COUNT(DISTINCT p.power_name) AS power_count
    FROM superhero AS h
    JOIN lightest_weight AS w ON h.weight_kg = w.weight_kg
    LEFT JOIN hero_power AS hp ON hp.hero_id = h.id
    LEFT JOIN superpower AS p ON p.id = hp.power_id
    GROUP BY h.id
)
SELECT MAX(power_count) AS max_power_count
FROM hero_power_counts;
