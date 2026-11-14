WITH PlayerMatchResults AS (
  SELECT
    p.player_api_id,
    m.match_id,
    t.team_long_name,
    CASE
      WHEN (m.home_team_api_id = t.team_api_id AND m.home_team_goal > m.away_team_goal) THEN 'win'
      WHEN (m.away_team_api_id = t.team_api_id AND m.away_team_goal > m.home_team_goal) THEN 'win'
      WHEN (m.home_team_api_id = t.team_api_id AND m.home_team_goal < m.away_team_goal) THEN 'lose'
      WHEN (m.away_team_api_id = t.team_api_id AND m.away_team_goal < m.home_team_goal) THEN 'lose'
      ELSE 'draw'
    END AS result
  FROM Player p
  JOIN Match m ON p.player_api_id IN 
    (SELECT player_api_id FROM Player_Attributes WHERE player_api_id = p.player_api_id)
  JOIN Team t ON p.team_api_id = t.team_api_id
  WHERE p.player_api_id IS NOT NULL
),
PlayerWinsLosses AS (
  SELECT
    player_api_id,
    SUM(CASE WHEN result = 'win' THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN result = 'lose' THEN 1 ELSE 0 END) AS losses
  FROM PlayerMatchResults
  WHERE result IN ('win', 'lose')
  GROUP BY player_api_id
),
MaxWins AS (
  SELECT MAX(wins) AS max_wins FROM PlayerWinsLosses
),
MaxLosses AS (
  SELECT MAX(losses) AS max_losses FROM PlayerWinsLosses
)
SELECT
  'Highest winning matches player' AS category,
  p.player_name,
  pwl.wins AS count
FROM PlayerWinsLosses pwl
JOIN Player p ON p.player_api_id = pwl.player_api_id
JOIN MaxWins mw ON pwl.wins = mw.max_wins

UNION ALL

SELECT
  'Highest losing matches player' AS category,
  p.player_name,
  pwl.losses AS count
FROM PlayerWinsLosses pwl
JOIN Player p ON p.player_api_id = pwl.player_api_id
JOIN MaxLosses ml ON pwl.losses = ml.max_losses;
