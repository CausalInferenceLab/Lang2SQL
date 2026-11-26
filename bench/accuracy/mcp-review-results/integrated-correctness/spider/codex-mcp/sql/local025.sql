WITH run_entries AS (
    SELECT match_id, innings_no, over_id, runs_scored AS runs
    FROM main.batsman_scored
    UNION ALL
    SELECT match_id, innings_no, over_id, extra_runs AS runs
    FROM main.extra_runs
),
over_totals AS (
    SELECT match_id, innings_no, over_id, SUM(runs) AS total_runs
    FROM run_entries
    GROUP BY match_id, innings_no, over_id
),
over_bowlers AS (
    SELECT match_id, innings_no, over_id,
           JSON_GROUP_ARRAY(DISTINCT bowler ORDER BY bowler) AS bowler_ids
    FROM main.ball_by_ball
    GROUP BY match_id, innings_no, over_id
),
ranked_overs AS (
    -- Include every match with recorded overs and take the earliest over among ties.
    SELECT b.match_id, b.innings_no, b.over_id, b.bowler_ids,
           COALESCE(t.total_runs, 0) AS total_runs,
           ROW_NUMBER() OVER (
               PARTITION BY b.match_id
               ORDER BY COALESCE(t.total_runs, 0) DESC, b.innings_no, b.over_id
           ) AS over_rank
    FROM over_bowlers AS b
    LEFT JOIN over_totals AS t USING (match_id, innings_no, over_id)
)
SELECT match_id, innings_no, over_id, total_runs, bowler_ids,
       AVG(total_runs) OVER () AS avg_highest_over_runs
FROM ranked_overs
WHERE over_rank = 1
ORDER BY match_id
LIMIT -1
