SELECT COUNT(*) AS character_count
FROM superhero AS s
LEFT JOIN race AS r ON r.id = s.race_id
WHERE s.publisher_id IN (
    SELECT publisher_id
    FROM superhero
    WHERE superhero_name = 'A-Bomb'
)
AND (r.race IS NULL OR r.race = '-');
