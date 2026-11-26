WITH scoring AS (
    SELECT match_id, innings_no, over_id, runs_scored AS runs
    FROM main.batsman_scored
    UNION ALL
    SELECT match_id, innings_no, over_id, extra_runs AS runs
    FROM main.extra_runs
),
ranked_overs AS (
    SELECT match_id, innings_no, over_id, SUM(runs) AS total_runs,
           ROW_NUMBER() OVER (
               PARTITION BY match_id
               -- Select the earliest over when totals tie.
               ORDER BY SUM(runs) DESC, innings_no, over_id
           ) AS over_rank
    FROM scoring
    GROUP BY match_id, innings_no, over_id
),
winning_over_bowlers AS (
    SELECT r.match_id, r.innings_no, r.over_id, r.total_runs,
           b.bowler, MIN(b.ball_id) AS first_ball
    FROM ranked_overs AS r
    LEFT JOIN main.ball_by_ball AS b USING (match_id, innings_no, over_id)
    WHERE r.over_rank = 1
    GROUP BY r.match_id, r.innings_no, r.over_id, r.total_runs, b.bowler
)
SELECT match_id, innings_no, over_id, total_runs,
       GROUP_CONCAT(bowler, ', ' ORDER BY first_ball) AS bowler_ids,
       AVG(total_runs) OVER () AS avg_highest_over_runs
FROM winning_over_bowlers
GROUP BY match_id, innings_no, over_id, total_runs
ORDER BY match_id
LIMIT -1
