WITH qualifying_players AS (
    SELECT DISTINCT player_api_id
    FROM Player_Attributes
    WHERE overall_rating > 70
)
SELECT ROUND(
    100.0 * COUNT(q.player_api_id) / NULLIF(COUNT(*), 0),
    2
) AS percentage
FROM Player AS p
LEFT JOIN qualifying_players AS q ON q.player_api_id = p.player_api_id
WHERE p.height < 180;
