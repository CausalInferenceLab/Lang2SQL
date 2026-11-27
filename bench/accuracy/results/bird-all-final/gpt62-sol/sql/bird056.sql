SELECT COUNT(DISTINCT c.uuid) AS printing_count
FROM cards AS c
JOIN (
    SELECT DISTINCT uuid
    FROM rulings
    WHERE text LIKE '%triggered ability%'
) AS r ON r.uuid = c.uuid
WHERE c.power IS NULL OR c.power LIKE '%*%';
