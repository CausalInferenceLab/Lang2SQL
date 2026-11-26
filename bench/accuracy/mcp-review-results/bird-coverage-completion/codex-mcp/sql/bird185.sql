WITH ranked_ratings AS (
    SELECT
        a.player_api_id,
        a.volleys,
        ROW_NUMBER() OVER (
            PARTITION BY a.player_api_id
            ORDER BY a.date DESC, a.id DESC
        ) AS rating_rank
    FROM main.Player_Attributes AS a
)
SELECT COUNT(*) AS player_count
FROM ranked_ratings AS r
JOIN main.Player AS p ON p.player_api_id = r.player_api_id
WHERE r.rating_rank = 1
  AND p.height > 180
  AND r.volleys > 70
LIMIT 1000
