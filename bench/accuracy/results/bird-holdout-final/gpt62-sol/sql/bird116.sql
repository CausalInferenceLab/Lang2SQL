SELECT COUNT(DISTINCT f.language) AS foreign_language_count
FROM cards AS c
JOIN foreign_data AS f ON f.uuid = c.uuid
WHERE c.name = 'Swamp';
