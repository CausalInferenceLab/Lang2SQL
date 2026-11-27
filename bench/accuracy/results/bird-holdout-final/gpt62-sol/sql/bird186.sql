WITH goals_by_player AS (
    SELECT home_player_3 AS player_api_id,
           SUM(home_team_goal) AS total_home_goals
    FROM "Match"
    GROUP BY home_player_3
)
SELECT ROUND(AVG(COALESCE(g.total_home_goals, 0)), 2) AS average_total_home_goals
FROM Player AS p
LEFT JOIN goals_by_player AS g ON g.player_api_id = p.player_api_id;
