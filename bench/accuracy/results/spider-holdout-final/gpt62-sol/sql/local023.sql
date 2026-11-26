WITH season_matches AS (
    SELECT DISTINCT match_id
    FROM "match"
    WHERE season_id = 5
), appearances AS (
    SELECT pm.player_id, COUNT(DISTINCT pm.match_id) AS matches_played
    FROM player_match AS pm
    JOIN season_matches AS sm ON sm.match_id = pm.match_id
    GROUP BY pm.player_id
), batting_runs AS (
    SELECT b.striker AS player_id, SUM(s.runs_scored) AS total_runs
    FROM ball_by_ball AS b
    JOIN season_matches AS sm ON sm.match_id = b.match_id
    JOIN batsman_scored AS s
      ON s.match_id = b.match_id
     AND s.innings_no = b.innings_no
     AND s.over_id = b.over_id
     AND s.ball_id = b.ball_id
    GROUP BY b.striker
), dismissals AS (
    SELECT w.player_out AS player_id, COUNT(*) AS times_out
    FROM wicket_taken AS w
    JOIN season_matches AS sm ON sm.match_id = w.match_id
    WHERE w.kind_out <> 'retired hurt'
    GROUP BY w.player_out
)
SELECT p.player_name,
       ROUND(1.0 * COALESCE(r.total_runs, 0) / NULLIF(d.times_out, 0), 2) AS batting_average
FROM appearances AS a
JOIN player AS p ON p.player_id = a.player_id
LEFT JOIN batting_runs AS r ON r.player_id = a.player_id
LEFT JOIN dismissals AS d ON d.player_id = a.player_id
ORDER BY 1.0 * COALESCE(r.total_runs, 0) / a.matches_played DESC,
         a.player_id
LIMIT 5;
