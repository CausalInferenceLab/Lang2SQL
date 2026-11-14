WITH striker_runs_per_match AS (
    SELECT batsman, match_id, SUM(runs_scored) AS total_runs
    FROM batsman_scored
    GROUP BY batsman, match_id
),
qualified_strikers AS (
    SELECT DISTINCT batsman
    FROM striker_runs_per_match
    WHERE total_runs > 50
),
avg_total_runs AS (
    SELECT batsman, SUM(runs_scored) AS total_runs_all_matches
    FROM batsman_scored
    WHERE batsman IN (SELECT batsman FROM qualified_strikers)
    GROUP BY batsman
)
SELECT AVG(total_runs_all_matches) AS average_total_runs
FROM avg_total_runs;
