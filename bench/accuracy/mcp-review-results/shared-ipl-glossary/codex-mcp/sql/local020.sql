WITH bowling_averages AS (
    SELECT
        b.bowler,
        1.0 * SUM(COALESCE(s.runs_scored, 0) + COALESCE(e.extra_runs, 0))
            / COUNT(w.player_out) AS bowling_average
    FROM main.ball_by_ball AS b
    LEFT JOIN main.batsman_scored AS s
        USING (match_id, innings_no, over_id, ball_id)
    LEFT JOIN main.extra_runs AS e
        ON (e.match_id, e.innings_no, e.over_id, e.ball_id)
         = (b.match_id, b.innings_no, b.over_id, b.ball_id)
        AND e.extra_type IN ('wides', 'noballs')
    LEFT JOIN main.wicket_taken AS w
        ON (w.match_id, w.innings_no, w.over_id, w.ball_id)
         = (b.match_id, b.innings_no, b.over_id, b.ball_id)
        AND w.kind_out IN ('bowled', 'caught', 'caught and bowled', 'hit wicket', 'lbw', 'stumped')
    GROUP BY b.bowler
    HAVING COUNT(w.player_out) > 0
)
SELECT p.player_name
FROM bowling_averages AS a
JOIN main.player AS p ON p.player_id = a.bowler
WHERE a.bowling_average = (SELECT MIN(bowling_average) FROM bowling_averages)
LIMIT 1000
