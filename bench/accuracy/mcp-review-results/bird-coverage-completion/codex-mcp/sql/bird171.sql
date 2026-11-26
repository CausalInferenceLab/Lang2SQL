WITH rated_above_70 AS (
    SELECT DISTINCT a.player_api_id
    FROM main.Player_Attributes AS a
    WHERE a.overall_rating > 70
)
SELECT ROUND(100.0 * COUNT(r.player_api_id) / NULLIF(COUNT(*), 0), 2) AS percentage
FROM main.Player AS p
LEFT JOIN rated_above_70 AS r ON r.player_api_id = p.player_api_id
WHERE p.height < 180
LIMIT 1000
