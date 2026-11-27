WITH role_counts AS (
    SELECT player_id, role, COUNT(DISTINCT match_id) AS role_matches
    FROM player_match
    GROUP BY player_id, role
),
ranked_roles AS (
    SELECT player_id, role,
           SUM(role_matches) OVER (PARTITION BY player_id) AS total_matches_played,
           ROW_NUMBER() OVER (
               PARTITION BY player_id ORDER BY role_matches DESC, role
           ) AS role_rank
    FROM role_counts
),
deliveries AS (
    SELECT b.match_id, b.innings_no, b.over_id, b.ball_id,
           b.striker, b.non_striker, b.bowler,
           COALESCE(s.runs_scored, 0) AS runs_scored
    FROM ball_by_ball AS b
    LEFT JOIN batsman_scored AS s
        USING (match_id, innings_no, over_id, ball_id)
),
batting_contributions AS (
    SELECT striker AS player_id, match_id,
           SUM(runs_scored) AS runs, COUNT(*) AS balls_faced
    FROM deliveries
    GROUP BY striker, match_id
    UNION ALL
    SELECT non_striker AS player_id, match_id, 0 AS runs, 0 AS balls_faced
    FROM deliveries
    GROUP BY non_striker, match_id
),
batting_by_match AS (
    SELECT player_id, match_id,
           SUM(runs) AS runs, SUM(balls_faced) AS balls_faced
    FROM batting_contributions
    GROUP BY player_id, match_id
),
batting_totals AS (
    SELECT player_id,
           SUM(runs) AS total_runs,
           MAX(runs) AS highest_score,
           COUNT(CASE WHEN runs >= 30 THEN 1 END) AS matches_30_plus,
           COUNT(CASE WHEN runs >= 50 THEN 1 END) AS matches_50_plus,
           COUNT(CASE WHEN runs >= 100 THEN 1 END) AS matches_100_plus,
           SUM(balls_faced) AS total_balls_faced
    FROM batting_by_match
    GROUP BY player_id
),
dismissals AS (
    SELECT player_out AS player_id, COUNT(*) AS total_dismissals
    FROM wicket_taken
    WHERE kind_out <> 'retired hurt'
    GROUP BY player_out
),
bowling_by_match AS (
    SELECT d.bowler AS player_id, d.match_id,
           SUM(d.runs_scored) AS runs_conceded,
           COUNT(*) AS balls_bowled,
           COUNT(CASE WHEN w.kind_out IN (
               'bowled', 'caught', 'caught and bowled', 'hit wicket', 'lbw', 'stumped'
           ) THEN 1 END) AS wickets
    FROM deliveries AS d
    LEFT JOIN wicket_taken AS w
        USING (match_id, innings_no, over_id, ball_id)
    GROUP BY d.bowler, d.match_id
),
ranked_bowling AS (
    SELECT player_id, wickets, runs_conceded,
           SUM(wickets) OVER (PARTITION BY player_id) AS total_wickets,
           6.0 * SUM(runs_conceded) OVER (PARTITION BY player_id)
               / NULLIF(SUM(balls_bowled) OVER (PARTITION BY player_id), 0) AS economy_rate,
           ROW_NUMBER() OVER (
               PARTITION BY player_id
               ORDER BY wickets DESC, runs_conceded ASC, match_id
           ) AS bowling_rank
    FROM bowling_by_match
)
SELECT p.player_id,
       p.player_name,
       r.role AS most_frequent_role,
       p.batting_hand,
       p.bowling_skill,
       COALESCE(b.total_runs, 0) AS total_runs,
       COALESCE(r.total_matches_played, 0) AS total_matches_played,
       COALESCE(d.total_dismissals, 0) AS total_dismissals,
       1.0 * COALESCE(b.total_runs, 0) / NULLIF(d.total_dismissals, 0) AS batting_average,
       b.highest_score,
       COALESCE(b.matches_30_plus, 0) AS matches_30_plus,
       COALESCE(b.matches_50_plus, 0) AS matches_50_plus,
       COALESCE(b.matches_100_plus, 0) AS matches_100_plus,
       COALESCE(b.total_balls_faced, 0) AS total_balls_faced,
       100.0 * b.total_runs / NULLIF(b.total_balls_faced, 0) AS strike_rate,
       COALESCE(w.total_wickets, 0) AS total_wickets,
       w.economy_rate,
       w.wickets || '-' || w.runs_conceded AS best_bowling_performance
FROM player AS p
LEFT JOIN ranked_roles AS r ON r.player_id = p.player_id AND r.role_rank = 1
LEFT JOIN batting_totals AS b ON b.player_id = p.player_id
LEFT JOIN dismissals AS d ON d.player_id = p.player_id
LEFT JOIN ranked_bowling AS w ON w.player_id = p.player_id AND w.bowling_rank = 1
ORDER BY p.player_id;
