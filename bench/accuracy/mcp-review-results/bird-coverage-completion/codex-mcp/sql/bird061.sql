WITH banned_printings AS (
    SELECT l.format, l.uuid
    FROM main.legalities AS l
    WHERE l.status = 'Banned'
),
format_counts AS (
    SELECT format, COUNT(*) AS printing_count
    FROM banned_printings
    GROUP BY format
)
SELECT DISTINCT b.format, c.name
FROM format_counts AS f
JOIN banned_printings AS b ON b.format = f.format
JOIN main.cards AS c ON c.uuid = b.uuid
WHERE f.printing_count = (SELECT MAX(printing_count) FROM format_counts)
ORDER BY b.format, c.name
LIMIT 1000
