SELECT
    p.player_api_id,
    p.player_name,
    AVG(a.heading_accuracy) AS mean_heading_accuracy
FROM Player AS p
JOIN Player_Attributes AS a
    ON a.player_api_id = p.player_api_id
WHERE p.height > 180
  AND a.heading_accuracy IS NOT NULL
GROUP BY p.player_api_id, p.player_name
ORDER BY mean_heading_accuracy DESC, p.player_api_id ASC
LIMIT 10;
