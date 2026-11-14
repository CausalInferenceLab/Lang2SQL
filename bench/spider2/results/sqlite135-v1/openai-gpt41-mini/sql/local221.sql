WITH WinCounts AS (
  SELECT home_team_api_id AS team_id, COUNT(*) AS wins
  FROM Match
  WHERE home_team_goal > away_team_goal
  GROUP BY home_team_api_id
  UNION ALL
  SELECT away_team_api_id AS team_id, COUNT(*) AS wins
  FROM Match
  WHERE away_team_goal > home_team_goal
  GROUP BY away_team_api_id
), TotalWins AS (
  SELECT team_id, SUM(wins) AS total_wins
  FROM WinCounts
  GROUP BY team_id
)
SELECT T.team_long_name, TW.total_wins
FROM TotalWins TW
JOIN Team T ON TW.team_id = T.team_api_id
ORDER BY TW.total_wins DESC
LIMIT 10;
