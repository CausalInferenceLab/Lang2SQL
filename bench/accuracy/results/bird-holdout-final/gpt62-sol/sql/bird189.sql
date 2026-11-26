WITH latest AS (
    SELECT preferred_foot,
           attacking_work_rate,
           ROW_NUMBER() OVER (
               PARTITION BY player_api_id
               ORDER BY date DESC, id DESC
           ) AS rn
    FROM Player_Attributes
)
SELECT ROUND(100.0 * COUNT(CASE WHEN attacking_work_rate = 'low' THEN 1 END)
             / NULLIF(COUNT(*), 0), 1) AS percentage
FROM latest
WHERE rn = 1
  AND preferred_foot = 'left'
  AND attacking_work_rate IN ('low', 'medium', 'high');
