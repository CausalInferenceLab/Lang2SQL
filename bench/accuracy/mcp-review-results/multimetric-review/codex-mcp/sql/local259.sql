WITH deliveries AS (
    -- Each delivery counts once. Only runs credited to the striker enter the score.
    SELECT b.match_id, b.striker, b.bowler,
           COALESCE(s.runs_scored, 0) AS runs, w.kind_out
    FROM main.ball_by_ball AS b
    LEFT JOIN main.batsman_scored AS s USING (match_id, innings_no, over_id, ball_id)
    LEFT JOIN main.wicket_taken AS w USING (match_id, innings_no, over_id, ball_id)
),
batting_matches AS (
    SELECT player_id, match_id, SUM(runs) AS match_runs, SUM(balls) AS balls_faced
    FROM (
        SELECT striker AS player_id, match_id, SUM(runs) AS runs, COUNT(*) AS balls
        FROM deliveries
        GROUP BY striker, match_id
        UNION ALL
        -- A non-striker can bat and finish a match without facing a delivery.
        SELECT DISTINCT non_striker, match_id, 0, 0
        FROM main.ball_by_ball
    ) AS batters
    GROUP BY player_id, match_id
),
batting AS (
    SELECT player_id, SUM(match_runs) AS runs, SUM(balls_faced) AS balls,
           MAX(match_runs) AS highest_match_score,
           COUNT(*) FILTER (WHERE match_runs >= 30) AS matches_30_plus,
           COUNT(*) FILTER (WHERE match_runs >= 50) AS matches_50_plus,
           COUNT(*) FILTER (WHERE match_runs >= 100) AS matches_100_plus
    FROM batting_matches
    GROUP BY player_id
),
dismissals AS (
    SELECT player_out AS player_id, COUNT(*) AS outs
    FROM main.wicket_taken
    WHERE kind_out <> 'retired hurt'
    GROUP BY player_out
),
bowling_matches AS (
    SELECT bowler AS player_id, match_id, SUM(runs) AS runs_given,
           COUNT(*) AS balls_bowled,
           COUNT(*) FILTER (WHERE kind_out IN (
               'bowled', 'caught', 'caught and bowled', 'hit wicket', 'lbw', 'stumped'
           )) AS wickets
    FROM deliveries
    GROUP BY bowler, match_id
),
bowling AS (
    SELECT player_id, wickets, runs_given,
           SUM(wickets) OVER career AS total_wickets,
           6.0 * SUM(runs_given) OVER career / SUM(balls_bowled) OVER career AS economy_rate,
           ROW_NUMBER() OVER (
               career ORDER BY wickets DESC, runs_given ASC, match_id
           ) AS performance_rank
    FROM bowling_matches
    WINDOW career AS (PARTITION BY player_id)
),
appearances AS (
    -- A roster entry or recorded participation establishes one player-match.
    SELECT player_id, match_id FROM main.player_match
    UNION
    SELECT player_id, match_id FROM batting_matches
    UNION
    SELECT player_id, match_id FROM bowling_matches
),
match_counts AS (
    SELECT player_id, COUNT(*) AS matches_played
    FROM appearances
    GROUP BY player_id
),
players AS (
    SELECT player_id FROM main.player
    UNION
    SELECT player_id FROM match_counts
),
role_counts AS (
    SELECT player_id, role,
           RANK() OVER (PARTITION BY player_id ORDER BY COUNT(*) DESC) AS frequency_rank
    FROM main.player_match
    GROUP BY player_id, role
),
roles AS (
    SELECT player_id, GROUP_CONCAT(role, ', ' ORDER BY role) AS most_frequent_role
    FROM role_counts
    WHERE frequency_rank = 1
    GROUP BY player_id
)
SELECT ids.player_id, p.player_name, r.most_frequent_role, p.batting_hand, p.bowling_skill,
       COALESCE(b.runs, 0) AS total_runs_scored,
       COALESCE(m.matches_played, 0) AS total_matches_played,
       COALESCE(d.outs, 0) AS total_dismissals,
       1.0 * COALESCE(b.runs, 0) / NULLIF(d.outs, 0) AS batting_average,
       b.highest_match_score,
       COALESCE(b.matches_30_plus, 0) AS matches_30_plus,
       COALESCE(b.matches_50_plus, 0) AS matches_50_plus,
       COALESCE(b.matches_100_plus, 0) AS matches_100_plus,
       COALESCE(b.balls, 0) AS total_balls_faced,
       100.0 * b.runs / NULLIF(b.balls, 0) AS strike_rate,
       COALESCE(w.total_wickets, 0) AS total_wickets_taken,
       w.economy_rate,
       w.wickets || '-' || w.runs_given AS best_bowling_performance
FROM players AS ids
LEFT JOIN main.player AS p USING (player_id)
LEFT JOIN roles AS r USING (player_id)
LEFT JOIN match_counts AS m USING (player_id)
LEFT JOIN batting AS b USING (player_id)
LEFT JOIN dismissals AS d USING (player_id)
LEFT JOIN bowling AS w ON w.player_id = ids.player_id AND w.performance_rank = 1
ORDER BY ids.player_id
LIMIT -1
