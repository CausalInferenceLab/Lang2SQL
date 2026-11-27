WITH ranked_ratings AS (
    SELECT player_api_id,
           volleys,
           ROW_NUMBER() OVER (
               PARTITION BY player_api_id
               ORDER BY date DESC, id DESC
           ) AS rating_rank
    FROM Player_Attributes
)
SELECT COUNT(DISTINCT p.player_api_id) AS player_count
FROM Player AS p
JOIN ranked_ratings AS r ON r.player_api_id = p.player_api_id
WHERE r.rating_rank = 1
  AND p.height > 180
  AND r.volleys > 70;
