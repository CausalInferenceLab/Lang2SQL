WITH bowler_overs AS (
    SELECT b.match_id, b.innings_no, b.over_id, b.bowler,
           SUM(COALESCE(s.runs_scored, 0) +
               CASE WHEN e.extra_type IN ('wides', 'noballs')
                    THEN COALESCE(e.extra_runs, 0) ELSE 0 END) AS runs_conceded
    FROM ball_by_ball AS b
    LEFT JOIN batsman_scored AS s
      ON s.match_id = b.match_id AND s.innings_no = b.innings_no
     AND s.over_id = b.over_id AND s.ball_id = b.ball_id
    LEFT JOIN extra_runs AS e
      ON e.match_id = b.match_id AND e.innings_no = b.innings_no
     AND e.over_id = b.over_id AND e.ball_id = b.ball_id
    GROUP BY b.match_id, b.innings_no, b.over_id, b.bowler
), over_totals AS (
    SELECT *, SUM(runs_conceded) OVER (
        PARTITION BY match_id, innings_no, over_id
    ) AS over_runs_conceded
    FROM bowler_overs
), match_maxima AS (
    SELECT *, MAX(over_runs_conceded) OVER (PARTITION BY match_id) AS match_max
    FROM over_totals
), bowler_bests AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY bowler
        ORDER BY runs_conceded DESC, match_id, innings_no, over_id
    ) AS bowler_rank
    FROM match_maxima
    WHERE over_runs_conceded = match_max
)
SELECT p.player_name AS bowler_name, b.match_id, b.runs_conceded
FROM bowler_bests AS b
JOIN player AS p ON p.player_id = b.bowler
WHERE b.bowler_rank = 1
ORDER BY b.runs_conceded DESC, b.bowler
LIMIT 3;
