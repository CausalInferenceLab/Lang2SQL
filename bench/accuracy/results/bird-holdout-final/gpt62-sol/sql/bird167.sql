WITH height_bounds AS (
    SELECT MIN(height) AS shortest_height, MAX(height) AS tallest_height
    FROM Player
), extreme_player_averages AS (
    SELECT p.player_api_id, p.player_name, AVG(pa.finishing) AS avg_finishing
    FROM Player AS p
    JOIN Player_Attributes AS pa ON pa.player_api_id = p.player_api_id
    CROSS JOIN height_bounds AS h
    WHERE p.height IN (h.shortest_height, h.tallest_height)
    GROUP BY p.player_api_id, p.player_name
)
SELECT player_name
FROM extreme_player_averages
WHERE avg_finishing = (SELECT MAX(avg_finishing) FROM extreme_player_averages);
