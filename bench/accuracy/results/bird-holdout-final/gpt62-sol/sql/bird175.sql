WITH ranked_ratings AS (
    SELECT player_api_id,
           defensive_work_rate,
           ROW_NUMBER() OVER (
               PARTITION BY player_api_id
               ORDER BY date DESC, id DESC
           ) AS rn
    FROM Player_Attributes
)
SELECT ROUND(AVG(p.height), 2) AS average_height_cm
FROM Player AS p
JOIN ranked_ratings AS r ON r.player_api_id = p.player_api_id
WHERE r.rn = 1
  AND r.defensive_work_rate IN ('low', 'high');
