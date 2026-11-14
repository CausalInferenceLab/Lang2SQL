WITH seasons AS (
  SELECT DISTINCT season FROM match
),
batsman_totals AS (
  SELECT 
    m.season,
    b.player_id,
    SUM(b.runs_scored) AS total_runs
  FROM ball_by_ball b
  JOIN match m ON b.match_id = m.match_id
  GROUP BY m.season, b.player_id
),
top_batsmen AS (
  SELECT 
    bt.season, 
    bt.player_id, 
    bt.total_runs,
    ROW_NUMBER() OVER (
      PARTITION BY bt.season 
      ORDER BY bt.total_runs DESC, bt.player_id ASC
    ) AS bat_rank
  FROM batsman_totals bt
),
wickets AS (
  SELECT 
    m.season,
    b.bowler_id,
    COUNT(*) AS wickets
  FROM ball_by_ball b
  JOIN match m ON b.match_id = m.match_id
  WHERE b.wicket_type IS NOT NULL 
    AND b.wicket_type NOT IN ('run out', 'hit wicket', 'retired hurt')
  GROUP BY m.season, b.bowler_id
),
top_bowlers AS (
  SELECT 
    w.season,
    w.bowler_id,
    w.wickets,
    ROW_NUMBER() OVER (
      PARTITION BY w.season 
      ORDER BY w.wickets DESC, w.bowler_id ASC
    ) AS bowl_rank
  FROM wickets w
)
SELECT 
  tb.season,
  tb.player_id AS batsman_id,
  tb.total_runs,
  pb.bowler_id,
  pb.wickets
FROM top_batsmen tb
JOIN top_bowlers pb
  ON tb.season = pb.season AND tb.bat_rank = pb.bowl_rank
WHERE tb.bat_rank <= 3 AND pb.bowl_rank <= 3
ORDER BY tb.season ASC, tb.bat_rank ASC;
