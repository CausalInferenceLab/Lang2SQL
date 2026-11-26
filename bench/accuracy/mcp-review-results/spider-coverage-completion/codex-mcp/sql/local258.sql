WITH match_figures AS (
    SELECT
        b.bowler,
        b.match_id,
        COUNT(w.match_id) AS wickets,
        SUM(COALESCE(s.runs_scored, 0)) AS bat_runs,
        COUNT(*) - COUNT(e.match_id) AS legal_balls
    FROM main.ball_by_ball AS b
    LEFT JOIN main.batsman_scored AS s
        USING (match_id, innings_no, over_id, ball_id)
    LEFT JOIN main.wicket_taken AS w
        ON w.match_id = b.match_id AND w.innings_no = b.innings_no
        AND w.over_id = b.over_id AND w.ball_id = b.ball_id
        AND w.kind_out IN ('bowled', 'caught', 'caught and bowled', 'hit wicket', 'lbw', 'stumped')
    LEFT JOIN main.extra_runs AS e
        ON e.match_id = b.match_id AND e.innings_no = b.innings_no
        AND e.over_id = b.over_id AND e.ball_id = b.ball_id
        AND e.extra_type IN ('wides', 'noballs')
    GROUP BY b.bowler, b.match_id
),
ranked_matches AS (
    SELECT
        match_figures.*,
        ROW_NUMBER() OVER (
            PARTITION BY bowler
            ORDER BY wickets DESC, bat_runs ASC, match_id ASC
        ) AS match_rank
    FROM match_figures
)
SELECT
    m.bowler AS bowler_id,
    p.player_name AS bowler_name,
    SUM(m.wickets) AS total_wickets,
    6.0 * SUM(m.bat_runs) / NULLIF(SUM(m.legal_balls), 0) AS economy_rate,
    1.0 * SUM(m.legal_balls) / NULLIF(SUM(m.wickets), 0) AS strike_rate,
    MAX(CASE WHEN m.match_rank = 1 THEN m.wickets || '-' || m.bat_runs END) AS best_bowling
FROM ranked_matches AS m
LEFT JOIN main.player AS p ON p.player_id = m.bowler
GROUP BY m.bowler, p.player_name
ORDER BY total_wickets DESC, m.bowler
LIMIT -1
