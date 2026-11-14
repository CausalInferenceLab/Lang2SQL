WITH bowler_balls AS (
    SELECT 
        b.ball_id,
        b.match_id,
        b.bowler_id,
        b.over_id,
        b.innings_no,
        binnings.ball_no,
        binnings.ball,
        binnings.innings_no as bin_innings_no
    FROM ball_by_ball b
    LEFT JOIN (
        SELECT DISTINCT match_id, over_id, innings_no, ball_no, ball
        FROM ball_by_ball
    ) binnings 
        ON b.match_id = binnings.match_id 
        AND b.over_id = binnings.over_id
        AND b.innings_no = binnings.innings_no
        AND b.ball_no = binnings.ball_no
),
valid_wickets AS (
    SELECT 
        b.bowler_id,
        b.match_id,
        COUNT(*) as wickets
    FROM wicket_taken w
    JOIN ball_by_ball b 
        ON w.ball_id = b.ball_id
    WHERE w.wicket_type NOT IN ('run out','retired hurt','obstructing the field','absent hurt') -- not bowler-only dismissals
    GROUP BY b.bowler_id, b.match_id
),
bowler_runs AS (
    SELECT 
        b.bowler_id,
        b.match_id,
        SUM(s.runs_scored) AS runs_conceded,
        COUNT(*) AS balls_bowled
    FROM ball_by_ball b
    LEFT JOIN batsman_scored s
        ON b.ball_id = s.ball_id
    WHERE b.ball_type NOT IN ('w','n') -- exclude wides/no balls for over and run calculation
    GROUP BY b.bowler_id, b.match_id
),
bowler_totals AS (
    SELECT 
        b.bowler_id,
        SUM(coalesce(br.runs_conceded,0)) AS total_runs_conceded,
        SUM(coalesce(br.balls_bowled,0)) AS total_balls_bowled,
        SUM(coalesce(vw.wickets,0)) AS total_wickets
    FROM player b
    LEFT JOIN bowler_runs br
        ON b.player_id = br.bowler_id
    LEFT JOIN valid_wickets vw
        ON b.player_id = vw.bowler_id 
        AND br.match_id = vw.match_id
    GROUP BY b.bowler_id
),
best_figures AS (
    SELECT 
        b.bowler_id,
        br.match_id,
        coalesce(vw.wickets,0) AS match_wickets,
        coalesce(br.runs_conceded,0) AS match_runs,
        ROW_NUMBER() OVER (
            PARTITION BY b.bowler_id 
            ORDER BY coalesce(vw.wickets,0) DESC, coalesce(br.runs_conceded,0) ASC
        ) as rn
    FROM player b
    LEFT JOIN bowler_runs br ON b.player_id = br.bowler_id
    LEFT JOIN valid_wickets vw ON b.player_id = vw.bowler_id AND br.match_id = vw.match_id
)
SELECT 
    p.player_id,
    p.player_name,
    bt.total_wickets,
    ROUND((CAST(bt.total_runs_conceded AS FLOAT) / NULLIF(bt.total_balls_bowled,0)) * 6, 2) AS economy_rate,
    ROUND(CASE WHEN bt.total_wickets > 0 THEN CAST(bt.total_balls_bowled AS FLOAT) / bt.total_wickets END, 2) AS strike_rate,
    printf('%d-%d', bf.match_wickets, bf.match_runs) as best_bowling
FROM player p
JOIN bowler_totals bt ON p.player_id = bt.bowler_id
JOIN best_figures bf ON p.player_id = bf.bowler_id AND bf.rn = 1
WHERE bt.total_balls_bowled > 0
ORDER BY bt.total_wickets DESC, economy_rate ASC
