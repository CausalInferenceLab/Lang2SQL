WITH match_winners AS (
    SELECT home_team_api_id AS team_api_id
    FROM "Match"
    WHERE home_team_goal > away_team_goal
    UNION ALL
    SELECT away_team_api_id AS team_api_id
    FROM "Match"
    WHERE away_team_goal > home_team_goal
)
SELECT t.team_long_name AS team, COUNT(w.team_api_id) AS wins
FROM Team AS t
LEFT JOIN match_winners AS w ON w.team_api_id = t.team_api_id
GROUP BY t.team_api_id, t.team_long_name
ORDER BY wins DESC, team ASC, t.team_api_id ASC
LIMIT 10;
