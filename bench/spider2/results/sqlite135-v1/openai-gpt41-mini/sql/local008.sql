WITH max_values AS (
  SELECT 
    MAX(g) AS max_games,
    MAX(r) AS max_runs,
    MAX(h) AS max_hits,
    MAX(hr) AS max_home_runs
  FROM batting
),
players_max_stats AS (
  SELECT p.name_given, b.g, b.r, b.h, b.hr
  FROM batting b
  JOIN player p ON b.player_id = p.player_id
  WHERE b.g = (SELECT max_games FROM max_values)
    OR b.r = (SELECT max_runs FROM max_values)
    OR b.h = (SELECT max_hits FROM max_values)
    OR b.hr = (SELECT max_home_runs FROM max_values)
)
SELECT DISTINCT name_given, g, r, h, hr
FROM players_max_stats;
