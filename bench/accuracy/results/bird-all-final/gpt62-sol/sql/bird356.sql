SELECT ROUND(AVG(s.height_cm), 1) AS average_height_cm
FROM superhero AS s
WHERE s.race_id IN (
    SELECT race_id
    FROM superhero
    WHERE superhero_name = 'A-Bomb'
)
AND s.height_cm > 0;
