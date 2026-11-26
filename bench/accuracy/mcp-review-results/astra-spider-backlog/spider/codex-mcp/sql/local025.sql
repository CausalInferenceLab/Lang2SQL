WITH scoring AS (
    SELECT match_id, innings_no, over_id, runs_scored AS runs
    FROM main.batsman_scored
    UNION ALL
    SELECT match_id, innings_no, over_id, extra_runs AS runs
    FROM main.extra_runs
),
over_totals AS (
    SELECT match_id, innings_no, over_id, SUM(runs) AS total_runs
    FROM scoring
    GROUP BY match_id, innings_no, over_id
),
ranked_overs AS (
    SELECT over_totals.*,
           ROW_NUMBER() OVER (
               PARTITION BY match_id
               ORDER BY total_runs DESC, innings_no, over_id
           ) AS over_rank
    FROM over_totals
)
SELECT r.match_id,
       r.innings_no,
       r.over_id,
       r.total_runs,
       GROUP_CONCAT(DISTINCT b.bowler ORDER BY b.bowler) AS bowler_ids,
       AVG(r.total_runs) OVER () AS average_highest_over_runs
FROM ranked_overs AS r
LEFT JOIN main.ball_by_ball AS b USING (match_id, innings_no, over_id)
WHERE r.over_rank = 1
GROUP BY r.match_id, r.innings_no, r.over_id, r.total_runs
ORDER BY r.match_id
LIMIT 1000
