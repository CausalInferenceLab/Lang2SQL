SELECT ROUND(AVG(p.height), 2) AS average_height_cm
FROM main.Player AS p
WHERE p.height > 180
  AND p.player_api_id IN (
      SELECT m.home_player_3
      FROM main."Match" AS m
  )
LIMIT 1000
