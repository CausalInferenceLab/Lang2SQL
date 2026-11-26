WITH commander_legal AS (
    SELECT DISTINCT uuid
    FROM legalities
    WHERE format = 'commander' AND status = 'Legal'
)
SELECT 100.0 * AVG(CASE WHEN c.hasContentWarning = 0 THEN 1.0 ELSE 0.0 END)
    AS percentage_without_content_warning
FROM cards AS c
JOIN commander_legal AS l ON l.uuid = c.uuid;
