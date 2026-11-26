SELECT 100.0 * COUNT(CASE WHEN c.hasContentWarning = 0 THEN 1 END)
       / NULLIF(COUNT(*), 0) AS percentage_without_content_warning
FROM cards AS c
WHERE c.uuid IN (
    SELECT l.uuid
    FROM legalities AS l
    WHERE l.format = 'commander' AND l.status = 'Legal'
)
LIMIT 1
