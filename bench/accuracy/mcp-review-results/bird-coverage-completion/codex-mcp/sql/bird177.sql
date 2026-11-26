WITH ranked_grades AS (
    SELECT p.height,
           a.defensive_work_rate,
           ROW_NUMBER() OVER (
               PARTITION BY p.player_api_id
               ORDER BY a.date DESC, a.id DESC
           ) AS recency
    FROM main.Player AS p
    JOIN main.Player_Attributes AS a ON a.player_api_id = p.player_api_id
    WHERE p.birthday >= '1990-01-01'
      AND p.birthday < '1996-01-01'
      AND a.defensive_work_rate IN ('low', 'medium', 'high')
)
SELECT ROUND(AVG(height), 2) AS average_height_cm
FROM ranked_grades
WHERE recency = 1 AND defensive_work_rate = 'medium'
LIMIT 1
