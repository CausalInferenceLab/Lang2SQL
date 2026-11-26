SELECT COUNT(*) AS printing_count
FROM (
    SELECT c.setCode, c.number
    FROM cards AS c
    JOIN foreign_data AS f ON f.uuid = c.uuid
    WHERE c.name = 'Counterspell'
      AND f.language IN ('French', 'Italian')
    GROUP BY c.setCode, c.number
    HAVING COUNT(DISTINCT f.language) = 2
) AS qualifying_printings
LIMIT 1000
