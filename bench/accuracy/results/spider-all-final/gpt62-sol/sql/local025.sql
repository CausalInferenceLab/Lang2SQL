WITH run_entries AS (
    SELECT match_id, innings_no, over_id, runs_scored AS runs
    FROM batsman_scored
    UNION ALL
    SELECT match_id, innings_no, over_id, extra_runs AS runs
    FROM extra_runs
), over_totals AS (
    SELECT match_id, innings_no, over_id, SUM(runs) AS total_runs
    FROM run_entries
    GROUP BY match_id, innings_no, over_id
), over_bowlers AS (
    SELECT match_id, innings_no, over_id,
           JSON_GROUP_ARRAY(DISTINCT bowler)
               FILTER (WHERE bowler IS NOT NULL) AS bowler_ids
    FROM ball_by_ball
    GROUP BY match_id, innings_no, over_id
), ranked_overs AS (
    SELECT b.match_id, b.innings_no, b.over_id, b.bowler_ids, t.total_runs,
           ROW_NUMBER() OVER (
               PARTITION BY b.match_id
               ORDER BY t.total_runs DESC, b.innings_no, b.over_id
           ) AS over_rank
    FROM over_bowlers AS b
    LEFT JOIN over_totals AS t USING (match_id, innings_no, over_id)
)
SELECT match_id, innings_no, over_id, bowler_ids,
       total_runs AS highest_over_runs,
       AVG(total_runs) OVER () AS avg_highest_over_runs
FROM ranked_overs
WHERE over_rank = 1
ORDER BY match_id;
