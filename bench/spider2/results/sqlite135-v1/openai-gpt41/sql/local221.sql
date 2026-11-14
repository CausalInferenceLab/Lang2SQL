SELECT 
    t.team_long_name AS team_name,
    COUNT(*) AS wins
FROM 
    Match m
JOIN 
    Team t ON m.home_team_api_id = t.team_api_id
WHERE 
    m.home_team_goal > m.away_team_goal
GROUP BY 
    t.team_api_id

UNION ALL

SELECT 
    t.team_long_name AS team_name,
    COUNT(*) AS wins
FROM 
    Match m
JOIN 
    Team t ON m.away_team_api_id = t.team_api_id
WHERE 
    m.away_team_goal > m.home_team_goal
GROUP BY 
    t.team_api_id

-- Aggregate wins for both home and away games
) AS all_wins
GROUP BY 
    team_name
ORDER BY 
    wins DESC
LIMIT 10;
