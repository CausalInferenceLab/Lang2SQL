WITH ranked_attributes AS (
    SELECT player_api_id, preferred_foot, attacking_work_rate,
           RANK() OVER (PARTITION BY player_api_id ORDER BY date DESC) AS recency
    FROM main.Player_Attributes
    WHERE date < '2015-01-02'
)
SELECT COUNT(DISTINCT player_api_id) AS player_count
FROM ranked_attributes
WHERE recency = 1
  AND preferred_foot = 'left'
  AND attacking_work_rate IN ('low', 'medium')
LIMIT 1
