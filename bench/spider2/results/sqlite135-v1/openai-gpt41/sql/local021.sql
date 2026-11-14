WITH striker_match_runs AS (
  SELECT
    bb.striker,
    bb.match_id,
    SUM(bs.runs_scored) AS total_runs
  FROM
    ball_by_ball bb
    JOIN batsman_scored bs ON bb.ball_id = bs.ball_id
  GROUP BY
    bb.striker,
    bb.match_id
)
SELECT
  AVG(total_runs) AS average_total_runs
FROM
  striker_match_runs
WHERE
  total_runs > 50;
