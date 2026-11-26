WITH bowler_overs AS (
    SELECT b.match_id, b.innings_no, b.over_id, b.bowler,
           SUM(COALESCE(s.runs_scored, 0) + COALESCE(e.extra_runs, 0)) AS runs_conceded
    FROM main.ball_by_ball AS b
    LEFT JOIN main.batsman_scored AS s
        USING (match_id, innings_no, over_id, ball_id)
    LEFT JOIN main.extra_runs AS e
        ON e.match_id = b.match_id AND e.innings_no = b.innings_no
       AND e.over_id = b.over_id AND e.ball_id = b.ball_id
       AND e.extra_type IN ('wides', 'noballs')
    GROUP BY b.match_id, b.innings_no, b.over_id, b.bowler
),
over_totals AS (
    SELECT bowler_overs.*,
           SUM(runs_conceded) OVER (PARTITION BY match_id, innings_no, over_id) AS over_runs
    FROM bowler_overs
),
ranked_overs AS (
    SELECT over_totals.*,
           RANK() OVER (PARTITION BY match_id ORDER BY over_runs DESC) AS match_rank
    FROM over_totals
),
qualifying_overs AS (
    SELECT match_id, bowler, runs_conceded
    FROM ranked_overs
    WHERE match_rank = 1
),
top_bowlers AS (
    SELECT bowler, MAX(runs_conceded) AS max_runs
    FROM qualifying_overs
    GROUP BY bowler
    ORDER BY max_runs DESC, bowler
    LIMIT 3
)
SELECT DISTINCT p.player_name AS bowler_name, q.match_id, t.max_runs AS runs_conceded
FROM top_bowlers AS t
JOIN qualifying_overs AS q ON q.bowler = t.bowler AND q.runs_conceded = t.max_runs
JOIN main.player AS p ON p.player_id = t.bowler
ORDER BY runs_conceded DESC, bowler_name, q.match_id
LIMIT 1000
