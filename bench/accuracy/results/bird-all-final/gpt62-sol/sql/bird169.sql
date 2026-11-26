WITH ranked_ratings AS (
    SELECT p.player_name,
           pa.overall_rating,
           ROW_NUMBER() OVER (
               PARTITION BY p.player_name
               ORDER BY pa.date DESC, pa.id DESC
           ) AS rn
    FROM Player AS p
    JOIN Player_Attributes AS pa ON pa.player_api_id = p.player_api_id
    WHERE p.player_name IN ('Ariel Borysiuk', 'Paulin Puel')
)
SELECT ROUND(100.0 * (a.overall_rating - b.overall_rating) / b.overall_rating, 2) AS percentage_higher
FROM ranked_ratings AS a
CROSS JOIN ranked_ratings AS b
WHERE a.player_name = 'Ariel Borysiuk' AND a.rn = 1
  AND b.player_name = 'Paulin Puel' AND b.rn = 1;
