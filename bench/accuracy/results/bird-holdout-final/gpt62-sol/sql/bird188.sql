WITH ranked_attributes AS (
    SELECT preferred_foot,
           attacking_work_rate,
           ROW_NUMBER() OVER (
               PARTITION BY player_api_id
               ORDER BY date DESC, id DESC
           ) AS rn
    FROM Player_Attributes
    WHERE date < '2015-01-02'
)
SELECT COUNT(*) AS player_count
FROM ranked_attributes
WHERE rn = 1
  AND preferred_foot = 'left'
  AND attacking_work_rate IN ('low', 'medium');
