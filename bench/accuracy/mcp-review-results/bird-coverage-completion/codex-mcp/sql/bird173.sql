SELECT ROUND(
    100.0 * COUNT(DISTINCT CASE WHEN a.defensive_work_rate = 'high' THEN p.player_api_id END)
    / NULLIF(COUNT(DISTINCT p.player_api_id), 0),
    2
) AS high_percentage
FROM main.Player AS p
JOIN main.Player_Attributes AS a ON a.player_api_id = p.player_api_id
WHERE p.birthday >= '1991-01-01'
  AND a.defensive_work_rate IN ('low', 'medium', 'high')
LIMIT 1
