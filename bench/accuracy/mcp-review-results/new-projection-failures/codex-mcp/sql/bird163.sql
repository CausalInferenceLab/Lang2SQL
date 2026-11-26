WITH away_wins AS (
    SELECT m.away_team_api_id, COUNT(*) AS wins
    FROM "Match" AS m
    JOIN League AS l ON l.id = m.league_id
    WHERE l.name = 'Scotland Premier League'
      AND m.season = '2009/2010'
      AND m.away_team_goal > m.home_team_goal
    GROUP BY m.away_team_api_id
)
SELECT t.team_long_name
FROM away_wins AS w
JOIN Team AS t ON t.team_api_id = w.away_team_api_id
WHERE w.wins = (SELECT MAX(wins) FROM away_wins)
ORDER BY t.team_long_name
LIMIT 1000
