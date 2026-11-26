WITH qualifying_scores AS (
    SELECT SUM(s.runs_scored) AS match_runs
    FROM ball_by_ball AS b
    JOIN batsman_scored AS s
      USING (match_id, innings_no, over_id, ball_id)
    GROUP BY b.match_id, b.striker
    HAVING SUM(s.runs_scored) > 50
)
SELECT AVG(match_runs) AS average_runs
FROM qualifying_scores;
