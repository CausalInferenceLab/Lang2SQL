WITH qualifying_printings AS (
    SELECT c.setCode, c.number
    FROM main.cards AS c
    JOIN main.foreign_data AS f ON f.uuid = c.uuid
    WHERE c.name = 'Counterspell'
      AND f.language IN ('French', 'Italian')
    GROUP BY c.setCode, c.number
    HAVING COUNT(DISTINCT f.language) = 2
)
SELECT COUNT(*) AS printing_count
FROM qualifying_printings
LIMIT 1000
