WITH max_stats AS (
  SELECT
    MAX(g) AS max_games,
    MAX(r) AS max_runs,
    MAX(h) AS max_hits,
    MAX(hr) AS max_home_runs
  FROM batting
),
games_leader AS (
  SELECT player_id, g AS value, 'games_played' AS stat_type
  FROM batting, max_stats
  WHERE g = max_games
),
runs_leader AS (
  SELECT player_id, r AS value, 'runs' AS stat_type
  FROM batting, max_stats
  WHERE r = max_runs
),
hits_leader AS (
  SELECT player_id, h AS value, 'hits' AS stat_type
  FROM batting, max_stats
  WHERE h = max_hits
),
hr_leader AS (
  SELECT player_id, hr AS value, 'home_runs' AS stat_type
  FROM batting, max_stats
  WHERE hr = max_home_runs
),
all_leaders AS (
  SELECT * FROM games_leader
  UNION
  SELECT * FROM runs_leader
  UNION
  SELECT * FROM hits_leader
  UNION
  SELECT * FROM hr_leader
)
SELECT
  p.given_name,
  a.stat_type,
  a.value
FROM all_leaders a
JOIN player p ON a.player_id = p.player_id
ORDER BY a.stat_type;
