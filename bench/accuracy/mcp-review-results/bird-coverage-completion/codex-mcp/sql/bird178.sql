SELECT ROUND(AVG(p.weight), 1) AS average_weight
FROM Player AS p
WHERE p.player_api_id IN (
    SELECT a.player_api_id
    FROM Player_Attributes AS a
    WHERE a.preferred_foot = 'left'
)
LIMIT 1000
