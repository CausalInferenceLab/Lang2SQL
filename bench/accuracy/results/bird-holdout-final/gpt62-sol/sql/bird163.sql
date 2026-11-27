WITH away_win_counts AS (
    SELECT m.away_team_api_id,
           SUM(CASE WHEN m.away_team_goal > m.home_team_goal THEN 1 ELSE 0 END) AS away_wins
    FROM "Match" AS m
    JOIN League AS l ON l.id = m.league_id
    WHERE l.name = 'Scotland Premier League'
      AND m.season = '2009/2010'
    GROUP BY m.away_team_api_id
)
SELECT t.team_long_name
FROM away_win_counts AS a
JOIN Team AS t ON t.team_api_id = a.away_team_api_id
WHERE a.away_wins = (SELECT MAX(away_wins) FROM away_win_counts)
ORDER BY t.team_long_name;
