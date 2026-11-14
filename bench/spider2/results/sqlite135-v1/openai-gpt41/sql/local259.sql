WITH player_roles AS (
    SELECT 
        pm.player_id,
        p.player_name,
        pm.role,
        COUNT(*) AS role_count
    FROM player_match pm
    JOIN player p ON pm.player_id = p.player_id
    GROUP BY pm.player_id, pm.role
),
most_frequent_role AS (
    SELECT 
        player_id,
        role
    FROM (
        SELECT 
            player_id,
            role,
            ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY role_count DESC, role ASC) as rn
        FROM player_roles
    ) 
    WHERE rn = 1
),
batting_stats AS (
    SELECT 
        p.player_id,
        SUM(COALESCE(bs.runs_scored, 0)) AS total_runs,
        COUNT(DISTINCT bbb.match_id) AS total_matches_played,
        COUNT(CASE WHEN wt.wicket_player_id = p.player_id THEN 1 END) AS total_dismissals,
        CASE 
            WHEN COUNT(CASE WHEN wt.wicket_player_id = p.player_id THEN 1 END) > 0 
            THEN 1.0 * SUM(COALESCE(bs.runs_scored, 0)) / COUNT(CASE WHEN wt.wicket_player_id = p.player_id THEN 1 END)
            ELSE NULL
        END AS batting_average,
        MAX(match_runs.max_runs) AS highest_score,
        SUM(CASE WHEN match_runs.max_runs >= 30 THEN 1 ELSE 0 END) AS matches_with_30plus,
        SUM(CASE WHEN match_runs.max_runs >= 50 THEN 1 ELSE 0 END) AS matches_with_50plus,
        SUM(CASE WHEN match_runs.max_runs >= 100 THEN 1 ELSE 0 END) AS matches_with_100plus,
        SUM(1) AS total_balls_faced,
        CASE 
            WHEN SUM(1) > 0 
            THEN 1.0 * SUM(COALESCE(bs.runs_scored,0)) / SUM(1) * 100
            ELSE NULL
        END AS strike_rate
    FROM player p
    LEFT JOIN batsman_scored bs ON p.player_id = bs.striker
    LEFT JOIN ball_by_ball bbb ON bs.match_id = bbb.match_id AND bs.innings_no = bbb.innings_no AND bs.over_id = bbb.over_id AND bs.ball_id = bbb.ball_id
    LEFT JOIN wicket_taken wt ON bs.match_id = wt.match_id AND bs.innings_no = wt.innings_no AND bs.over_id = wt.over_id AND bs.ball_id = wt.ball_id
    -- Above: Linking dismissals and balls
    LEFT JOIN (
        SELECT 
            bs.striker,
            bs.match_id,
            SUM(COALESCE(bs.runs_scored, 0)) as max_runs
        FROM batsman_scored bs
        GROUP BY bs.striker, bs.match_id
    ) match_runs ON p.player_id = match_runs.striker
    GROUP BY p.player_id
),
bowling_stats AS (
    SELECT 
        p.player_id,
        COUNT(CASE WHEN wt.wicket_taker_id = p.player_id THEN 1 END) AS total_wickets,
        SUM(COALESCE(bs.runs_scored, 0)) AS runs_conceded,
        COUNT(CASE WHEN bbb.bowler = p.player_id THEN 1 END) AS balls_bowled,
        CASE 
            WHEN COUNT(CASE WHEN bbb.bowler = p.player_id THEN 1 END) > 0 
            THEN 1.0 * SUM(COALESCE(bs.runs_scored, 0)) / COUNT(CASE WHEN bbb.bowler = p.player_id THEN 1 END) * 6
            ELSE NULL
        END AS economy_rate
    FROM player p
    LEFT JOIN ball_by_ball bbb ON p.player_id = bbb.bowler
    LEFT JOIN batsman_scored bs 
        ON bbb.match_id = bs.match_id 
        AND bbb.innings_no = bs.innings_no 
        AND bbb.over_id = bs.over_id
        AND bbb.ball_id = bs.ball_id
    LEFT JOIN wicket_taken wt ON bbb.match_id = wt.match_id AND bbb.innings_no = wt.innings_no AND bbb.over_id = wt.over_id AND bbb.ball_id = wt.ball_id
    GROUP BY p.player_id
),
bowling_best AS (
    -- Best bowling: for each match, per player, get wickets taken and runs conceded as bowler. Best is highest wickets, then lowest runs.
    SELECT 
        bbb.bowler AS player_id,
        bbb.match_id,
        COUNT(DISTINCT CASE WHEN wt.wicket_taker_id = bbb.bowler THEN wt.wicket_player_id END) as wickets,
        SUM(COALESCE(bs.runs_scored,0)) as runs_given
    FROM ball_by_ball bbb
    LEFT JOIN wicket_taken wt ON bbb.match_id = wt.match_id AND bbb.innings_no = wt.innings_no AND bbb.over_id = wt.over_id AND bbb.ball_id = wt.ball_id AND wt.wicket_taker_id = bbb.bowler
    LEFT JOIN batsman_scored bs 
        ON bbb.match_id = bs.match_id 
        AND bbb.innings_no = bs.innings_no 
        AND bbb.over_id = bs.over_id
        AND bbb.ball_id = bs.ball_id
    GROUP BY bbb.bowler, bbb.match_id
),
best_bowling_perf AS (
    SELECT player_id,
           (wickets || '-' || runs_given) AS best_bowling
    FROM (
        SELECT player_id, wickets, runs_given,
               ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY wickets DESC, runs_given ASC) as rn
        FROM bowling_best
    ) WHERE rn = 1
)
SELECT 
    p.player_id,
    p.player_name,
    mfr.role AS most_frequent_role,
    p.batting_hand,
    p.bowling_skill,
    bs.total_runs,
    bs.total_matches_played,
    bs.total_dismissals,
    bs.batting_average,
    bs.highest_score,
    bs.matches_with_30plus,
    bs.matches_with_50plus,
    bs.matches_with_100plus,
    bs.total_balls_faced,
    bs.strike_rate,
    bls.total_wickets,
    bls.economy_rate,
    COALESCE(bp.best_bowling, NULL) AS best_bowling_performance
FROM player p
LEFT JOIN most_frequent_role mfr ON p.player_id = mfr.player_id
LEFT JOIN batting_stats bs ON p.player_id = bs.player_id
LEFT JOIN bowling_stats bls ON p.player_id = bls.player_id
LEFT JOIN best_bowling_perf bp ON p.player_id = bp.player_id
ORDER BY p.player_id
;
