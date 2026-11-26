SELECT p.player_name
FROM main.Player AS p
JOIN main.Player_Attributes AS a
    ON a.player_api_id = p.player_api_id
WHERE p.height > 180
    AND a.heading_accuracy IS NOT NULL
GROUP BY p.player_api_id, p.player_name
ORDER BY AVG(a.heading_accuracy) DESC, p.player_api_id ASC
LIMIT 10
