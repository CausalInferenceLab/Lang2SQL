WITH bowler_wickets AS (
    SELECT 
        wt.player_out AS out_player_id,
        bb.bowler,
        wt.match_id,
        wt.over_id,
        wt.ball_id
    FROM wicket_taken wt
    JOIN ball_by_ball bb ON wt.match_id = bb.match_id AND wt.over_id = bb.over_id AND wt.ball_id = bb.ball_id
    WHERE wt.kind_out != 'run out'
),

bowler_runs_balls AS (
    SELECT 
        bb.bowler,
        bb.match_id,
        COUNT(*) AS balls_bowled,
        SUM(bs.runs_scored) AS runs_off_bat
    FROM ball_by_ball bb
    LEFT JOIN batsman_scored bs ON bb.match_id = bs.match_id AND bb.over_id = bs.over_id AND bb.ball_id = bs.ball_id
    LEFT JOIN extra_runs er ON bb.match_id = er.match_id AND bb.over_id = er.over_id AND bb.ball_id = er.ball_id
    WHERE er.extra_runs IS NULL
    GROUP BY bb.bowler, bb.match_id
),

bowler_match_stats AS (
    SELECT 
        bw.bowler,
        bw.match_id,
        COUNT(bw.ball_id) AS wickets,
        COALESCE(brb.runs_off_bat, 0) AS runs_conceded,
        COALESCE(brb.balls_bowled, 0) AS balls_bowled
    FROM bowler_wickets bw
    LEFT JOIN bowler_runs_balls brb ON bw.bowler = brb.bowler AND bw.match_id = brb.match_id
    GROUP BY bw.bowler, bw.match_id
),

bowler_totals AS (
    SELECT 
        bowler,
        SUM(wickets) AS total_wickets,
        SUM(runs_conceded) AS total_runs_conceded,
        SUM(balls_bowled) AS total_balls_bowled
    FROM bowler_match_stats
    GROUP BY bowler
),

best_bowling AS (
    SELECT bowler, match_id, wickets, runs_conceded,
        ROW_NUMBER() OVER (PARTITION BY bowler ORDER BY wickets DESC, runs_conceded ASC) AS rn
    FROM bowler_match_stats
),

best_perf AS (
    SELECT bowler, match_id, wickets, runs_conceded
    FROM best_bowling
    WHERE rn = 1
)

SELECT
    p.player_id AS bowler_id,
    p.player_name,
    bt.total_wickets,
    CASE WHEN bt.total_balls_bowled > 0 THEN CAST(bt.total_runs_conceded AS FLOAT) / (bt.total_balls_bowled / 6) ELSE NULL END AS economy_rate,
    CASE WHEN bt.total_wickets > 0 THEN CAST(bt.total_balls_bowled AS FLOAT) / bt.total_wickets ELSE NULL END AS strike_rate,
    bp.wickets || '-' || bp.runs_conceded AS best_bowling_performance
FROM bowler_totals bt
JOIN best_perf bp ON bt.bowler = bp.bowler
JOIN player p ON p.player_id = bt.bowler
ORDER BY bt.total_wickets DESC
LIMIT 1000;
