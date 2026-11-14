WITH PlayerRoles AS (
  SELECT player_id, role, COUNT(*) AS role_count
  FROM player_match
  GROUP BY player_id, role
),
MostFrequentRole AS (
  SELECT player_id, role AS most_frequent_role
  FROM (
    SELECT player_id, role, role_count, 
           ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY role_count DESC) AS rn
    FROM PlayerRoles
  ) 
  WHERE rn = 1
),
BatsmanRuns AS (
  SELECT 
    match_id,
    over_id,
    ball_id,
    runs_scored
  FROM batsman_scored
),
BallsFaced AS (
  SELECT 
    b.match_id, b.over_id, b.ball_id, b.striker AS player_id
  FROM ball_by_ball b
),
PlayerBattingStats AS (
  SELECT
    bf.player_id,
    SUM(br.runs_scored) AS total_runs,
    COUNT(DISTINCT br.match_id) AS matches_played,
    COUNT(wt.ball_id) AS total_dismissals,
    MAX(br.runs_scored) AS highest_score,
    COUNT(DISTINCT CASE WHEN br.runs_scored >= 30 THEN br.match_id END) AS matches_30_or_more,
    COUNT(DISTINCT CASE WHEN br.runs_scored >= 50 THEN br.match_id END) AS matches_50_or_more,
    COUNT(DISTINCT CASE WHEN br.runs_scored >= 100 THEN br.match_id END) AS matches_100_or_more,
    COUNT(*) AS total_balls_faced
  FROM BallsFaced bf
  LEFT JOIN BatsmanRuns br ON bf.match_id = br.match_id AND bf.over_id = br.over_id AND bf.ball_id = br.ball_id
  LEFT JOIN wicket_taken wt ON bf.player_id = wt.player_out AND bf.match_id = wt.match_id
  GROUP BY bf.player_id
),
BattingAverage AS (
  SELECT player_id, CASE WHEN total_dismissals = 0 THEN NULL ELSE CAST(total_runs AS FLOAT) / total_dismissals END AS batting_average
  FROM PlayerBattingStats
),
StrikeRate AS (
  SELECT player_id, CASE WHEN total_balls_faced = 0 THEN NULL ELSE (total_runs * 100.0 / total_balls_faced) END AS strike_rate
  FROM PlayerBattingStats
),
BowlingStatsPerBall AS (
  SELECT
    bb.bowler AS player_id,
    bb.match_id,
    COUNT(*) AS balls_bowled,
    SUM(COALESCE(bs.runs_scored,0)) AS runs_off_bat,
    0 AS extras_runs,
    SUM(COALESCE(wt2.player_out IS NOT NULL, 0)) AS wickets_in_ball
  FROM ball_by_ball bb
  LEFT JOIN batsman_scored bs ON bb.match_id = bs.match_id AND bb.over_id = bs.over_id AND bb.ball_id = bs.ball_id
  LEFT JOIN wicket_taken wt2 ON bb.match_id = wt2.match_id AND bb.over_id = wt2.over_id AND bb.ball_id = wt2.ball_id
  GROUP BY bb.bowler, bb.match_id, bb.over_id, bb.ball_id
),
BowlingStats AS (
  SELECT
    player_id,
    SUM(wickets_in_ball) AS total_wickets,
    SUM(runs_off_bat) AS runs_conceded,
    SUM(balls_bowled) AS balls_bowled
  FROM BowlingStatsPerBall
  GROUP BY player_id
),
EconomyRate AS (
  SELECT player_id, CASE WHEN balls_bowled = 0 THEN NULL ELSE runs_conceded * 6.0 / balls_bowled END AS economy_rate
  FROM BowlingStats
),
BowlingPerformancePerMatch AS (
  SELECT
    player_id,
    match_id,
    SUM(wickets_in_ball) AS wickets_in_match,
    SUM(runs_off_bat) AS runs_given
  FROM BowlingStatsPerBall
  GROUP BY player_id, match_id
),
BestBowlingPerformance AS (
  SELECT
    player_id,
    MAX(wickets_in_match) AS max_wickets
  FROM BowlingPerformancePerMatch
  GROUP BY player_id
),
BestBowlingRunsGiven AS (
  SELECT
    bp.player_id,
    MIN(bp2.runs_given) AS min_runs_given
  FROM BestBowlingPerformance bp
  JOIN BowlingPerformancePerMatch bp2 ON bp.player_id = bp2.player_id AND bp.max_wickets = bp2.wickets_in_match
  GROUP BY bp.player_id
),
BestBowlingPerformanceFormatted AS (
  SELECT
    bwp.player_id,
    printf('%d-%d', bwp.max_wickets, bbr.min_runs_given) AS best_bowling_performance
  FROM BestBowlingPerformance bwp
  JOIN BestBowlingRunsGiven bbr ON bwp.player_id = bbr.player_id
),
MostFrequentRoleByPlayer AS (
  SELECT player_id, most_frequent_role
  FROM MostFrequentRole
),
PlayersAggregated AS (
  SELECT
    p.player_id,
    p.player_name,
    fr.most_frequent_role,
    p.batting_hand,
    p.bowling_skill
  FROM player p
  LEFT JOIN MostFrequentRoleByPlayer fr ON p.player_id = fr.player_id
)
SELECT
  pa.player_id,
  pa.player_name,
  pa.most_frequent_role,
  pa.batting_hand,
  pa.bowling_skill,
  COALESCE(pb.total_runs, 0) AS total_runs_scored,
  COALESCE(pb.matches_played, 0) AS total_matches_played,
  COALESCE(pb.total_dismissals, 0) AS total_times_dismissed,
  ba.batting_average,
  COALESCE(pb.highest_score, 0) AS highest_score,
  COALESCE(pb.matches_30_or_more, 0) AS matches_30_or_more,
  COALESCE(pb.matches_50_or_more, 0) AS matches_50_or_more,
  COALESCE(pb.matches_100_or_more, 0) AS matches_100_or_more,
  COALESCE(pb.total_balls_faced, 0) AS total_balls_faced,
  sr.strike_rate,
  COALESCE(bs.total_wickets, 0) AS total_wickets_taken,
  er.economy_rate,
  bbpf.best_bowling_performance
FROM PlayersAggregated pa
LEFT JOIN PlayerBattingStats pb ON pa.player_id = pb.player_id
LEFT JOIN BattingAverage ba ON pa.player_id = ba.player_id
LEFT JOIN StrikeRate sr ON pa.player_id = sr.player_id
LEFT JOIN BowlingStats bs ON pa.player_id = bs.player_id
LEFT JOIN EconomyRate er ON pa.player_id = er.player_id
LEFT JOIN BestBowlingPerformanceFormatted bbpf ON pa.player_id = bbpf.player_id
ORDER BY pa.player_id
LIMIT 1000;
