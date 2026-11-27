WITH striker_match_runs AS (
    SELECT b.striker, b.match_id, SUM(s.runs_scored) AS match_runs
    FROM ball_by_ball AS b
    JOIN batsman_scored AS s
      ON s.match_id = b.match_id
     AND s.innings_no = b.innings_no
     AND s.over_id = b.over_id
     AND s.ball_id = b.ball_id
    WHERE b.striker IS NOT NULL
    GROUP BY b.striker, b.match_id
), qualifying_striker_totals AS (
    SELECT striker, SUM(match_runs) AS total_runs
    FROM striker_match_runs
    GROUP BY striker
    HAVING MAX(match_runs) > 50
)
SELECT AVG(total_runs) AS average_total_runs
FROM qualifying_striker_totals;
