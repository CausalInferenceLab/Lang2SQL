WITH bowler_totals AS (
    SELECT
        b.bowler AS player_id,
        SUM(COALESCE(s.runs_scored, 0) +
            CASE WHEN e.extra_type IN ('wides', 'noballs')
                 THEN COALESCE(e.extra_runs, 0) ELSE 0 END) AS runs_conceded,
        SUM(CASE WHEN w.kind_out IN ('bowled', 'caught', 'caught and bowled',
                                    'hit wicket', 'lbw', 'stumped')
                 THEN 1 ELSE 0 END) AS wickets
    FROM ball_by_ball AS b
    LEFT JOIN batsman_scored AS s USING (match_id, over_id, ball_id, innings_no)
    LEFT JOIN extra_runs AS e USING (match_id, over_id, ball_id, innings_no)
    LEFT JOIN wicket_taken AS w USING (match_id, over_id, ball_id, innings_no)
    GROUP BY b.bowler
), ranked_bowlers AS (
    SELECT p.player_name,
           RANK() OVER (ORDER BY 1.0 * t.runs_conceded / t.wickets) AS average_rank
    FROM bowler_totals AS t
    JOIN player AS p ON p.player_id = t.player_id
    WHERE t.wickets > 0
)
SELECT player_name
FROM ranked_bowlers
WHERE average_rank = 1
ORDER BY player_name;
