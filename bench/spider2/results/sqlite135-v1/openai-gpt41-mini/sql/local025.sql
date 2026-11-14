WITH OverRuns AS (
    SELECT
      b.match_id,
      b.innings_no,
      b.over_id,
      SUM(COALESCE(bs.runs_scored, 0)) AS batsman_runs,
      SUM(COALESCE(er.extra_runs, 0)) AS extra_runs
    FROM
      ball_by_ball b
      LEFT JOIN batsman_scored bs ON b.match_id = bs.match_id AND b.innings_no = bs.innings_no AND b.over_id = bs.over_id
      LEFT JOIN extra_runs er ON b.match_id = er.match_id AND b.innings_no = er.innings_no AND b.over_id = er.over_id
    GROUP BY
      b.match_id, b.innings_no, b.over_id
), MaxOverRuns AS (
    SELECT
      match_id, innings_no, over_id, batsman_runs + extra_runs AS total_runs
    FROM
      OverRuns
    WHERE
      (match_id, innings_no, batsman_runs + extra_runs) IN (
        SELECT
          match_id, innings_no, MAX(batsman_runs + extra_runs)
        FROM
          OverRuns
        GROUP BY
          match_id, innings_no
      )
), MaxBowler AS (
    SELECT
      mor.match_id, mor.innings_no, mor.over_id, mor.total_runs, b.bowler
    FROM
      MaxOverRuns mor
      JOIN ball_by_ball b ON b.match_id = mor.match_id AND b.innings_no = mor.innings_no AND b.over_id = mor.over_id
    GROUP BY
      mor.match_id, mor.innings_no, mor.over_id, mor.total_runs, b.bowler
)
SELECT
  AVG(total_runs) AS avg_highest_run_over
FROM
  MaxBowler;
