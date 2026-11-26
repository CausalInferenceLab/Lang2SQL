WITH banned_printings AS (
    SELECT l.format, c.name AS card_name
    FROM legalities AS l
    JOIN cards AS c ON c.uuid = l.uuid
    WHERE l.status = 'Banned'
), format_counts AS (
    SELECT format, COUNT(*) AS banned_printing_count
    FROM banned_printings
    GROUP BY format
)
SELECT DISTINCT b.format, b.card_name
FROM banned_printings AS b
JOIN format_counts AS f ON f.format = b.format
WHERE f.banned_printing_count = (SELECT MAX(banned_printing_count) FROM format_counts)
ORDER BY b.format, b.card_name;
