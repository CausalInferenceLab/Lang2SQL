WITH role_stats AS (
    SELECT pm.player_id, pm.role,
           SUM(COUNT(*)) OVER (PARTITION BY pm.player_id) AS matches_played,
           ROW_NUMBER() OVER (
               PARTITION BY pm.player_id
               -- Choose the alphabetically first role when match counts tie.
               ORDER BY COUNT(*) DESC, pm.role ASC
           ) AS role_rank
    FROM main.player_match AS pm
    GROUP BY pm.player_id, pm.role
),
deliveries AS (
    -- Count every recorded delivery and take runs from the batting score.
    SELECT b.match_id, b.striker, b.bowler,
           COALESCE(s.runs_scored, 0) AS runs,
           CASE WHEN w.kind_out IN (
               'bowled', 'caught', 'caught and bowled', 'hit wicket', 'lbw', 'stumped'
           ) THEN 1 ELSE 0 END AS bowler_wicket
    FROM main.ball_by_ball AS b
    LEFT JOIN main.batsman_scored AS s USING (match_id, innings_no, over_id, ball_id)
    LEFT JOIN main.wicket_taken AS w USING (match_id, innings_no, over_id, ball_id)
),
batting_by_match AS (
    SELECT player_id, match_id, SUM(runs) AS match_runs, SUM(balls_faced) AS balls_faced
    FROM (
        SELECT striker AS player_id, match_id, runs, 1 AS balls_faced
        FROM deliveries
        UNION ALL
        -- Include batters who appeared only at the non-striker's end.
        SELECT DISTINCT non_striker, match_id, 0, 0
        FROM main.ball_by_ball
    ) AS batters
    GROUP BY player_id, match_id
),
batting AS (
    SELECT player_id, SUM(match_runs) AS total_runs, MAX(match_runs) AS highest_match_score,
           COUNT(*) FILTER (WHERE match_runs >= 30) AS matches_30_plus,
           COUNT(*) FILTER (WHERE match_runs >= 50) AS matches_50_plus,
           COUNT(*) FILTER (WHERE match_runs >= 100) AS matches_100_plus,
           SUM(balls_faced) AS total_balls_faced
    FROM batting_by_match
    GROUP BY player_id
),
dismissals AS (
    SELECT player_out AS player_id, COUNT(*) AS total_dismissals
    FROM main.wicket_taken
    WHERE kind_out <> 'retired hurt'
    GROUP BY player_out
),
bowling_by_match AS (
    SELECT bowler AS player_id, match_id, SUM(bowler_wicket) AS wickets,
           SUM(runs) AS runs_given, COUNT(*) AS balls_bowled
    FROM deliveries
    GROUP BY bowler, match_id
),
bowling AS (
    SELECT player_id, wickets, runs_given,
           SUM(wickets) OVER career AS total_wickets,
           6.0 * SUM(runs_given) OVER career
               / NULLIF(SUM(balls_bowled) OVER career, 0) AS economy_rate,
           ROW_NUMBER() OVER (
               career ORDER BY wickets DESC, runs_given ASC, match_id ASC
           ) AS performance_rank
    FROM bowling_by_match
    WINDOW career AS (PARTITION BY player_id)
)
SELECT p.player_id, p.player_name, r.role AS most_frequent_role,
       p.batting_hand, p.bowling_skill,
       COALESCE(bt.total_runs, 0) AS total_runs_scored,
       COALESCE(r.matches_played, 0) AS total_matches_played,
       COALESCE(d.total_dismissals, 0) AS total_dismissals,
       1.0 * COALESCE(bt.total_runs, 0) / NULLIF(d.total_dismissals, 0) AS batting_average,
       bt.highest_match_score,
       COALESCE(bt.matches_30_plus, 0) AS matches_30_plus,
       COALESCE(bt.matches_50_plus, 0) AS matches_50_plus,
       COALESCE(bt.matches_100_plus, 0) AS matches_100_plus,
       COALESCE(bt.total_balls_faced, 0) AS total_balls_faced,
       100.0 * bt.total_runs / NULLIF(bt.total_balls_faced, 0) AS strike_rate,
       COALESCE(bw.total_wickets, 0) AS total_wickets_taken,
       bw.economy_rate,
       bw.wickets || '-' || bw.runs_given AS best_bowling_performance
-- Match records for ID 35 lack a corresponding player profile.
FROM main.player AS p
LEFT JOIN role_stats AS r ON r.player_id = p.player_id AND r.role_rank = 1
LEFT JOIN batting AS bt ON bt.player_id = p.player_id
LEFT JOIN dismissals AS d ON d.player_id = p.player_id
LEFT JOIN bowling AS bw ON bw.player_id = p.player_id AND bw.performance_rank = 1
ORDER BY p.player_id
LIMIT -1
