WITH qualifying_sets AS (
    SELECT s.id, s.totalSetSize
    FROM main.sets AS s
    LEFT JOIN main.set_translations AS t ON t.setCode = s.code
    GROUP BY s.id, s.totalSetSize
    HAVING COUNT(DISTINCT t.translation) < 5
)
SELECT SUM(totalSetSize) AS total_cards
FROM qualifying_sets
LIMIT 1
