SELECT
    p.player_name,
    AVG(pa.heading_accuracy) AS mean_heading_accuracy
FROM Player AS p
JOIN Player_Attributes AS pa ON pa.player_api_id = p.player_api_id
WHERE p.height > 180
  AND pa.heading_accuracy IS NOT NULL
GROUP BY p.player_api_id, p.player_name
ORDER BY mean_heading_accuracy DESC, p.player_api_id ASC
LIMIT 10
