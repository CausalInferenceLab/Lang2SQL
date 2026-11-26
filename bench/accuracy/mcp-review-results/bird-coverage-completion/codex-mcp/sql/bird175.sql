WITH ranked_ratings AS (
    SELECT player_api_id, defensive_work_rate,
           RANK() OVER (PARTITION BY player_api_id ORDER BY date DESC) AS recency
    FROM main.Player_Attributes
)
SELECT ROUND(AVG(p.height), 2) AS average_height_cm
FROM main.Player AS p
WHERE p.player_api_id IN (
    SELECT player_api_id
    FROM ranked_ratings
    WHERE recency = 1 AND defensive_work_rate IN ('low', 'high')
)
LIMIT 1
