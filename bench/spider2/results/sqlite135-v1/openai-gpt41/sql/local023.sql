WITH season_matches AS (
  SELECT id AS match_id
  FROM match
  WHERE season = 5
),
player_match_stats AS (
  SELECT
    pm.player_id,
    COUNT(pm.match_id) AS matches_played,
    SUM(bs.runs_scored) AS total_runs
  FROM player_match pm
  JOIN season_matches sm ON pm.match_id = sm.match_id
  JOIN batsman_scored bs ON bs.match_id = pm.match_id AND bs.batsman_id = pm.player_id
  GROUP BY pm.player_id
),
batting_averages AS (
  SELECT
    pms.player_id,
    pl.name,
    CAST(pms.total_runs AS FLOAT) / pms.matches_played AS avg_runs_per_match
  FROM player_match_stats pms
  JOIN player pl ON pms.player_id = pl.id
  WHERE pms.matches_played > 0
)
SELECT
  name,
  avg_runs_per_match
FROM batting_averages
ORDER BY avg_runs_per_match DESC
LIMIT 5;
