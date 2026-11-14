WITH over_runs AS (
    SELECT
        bb.match_id,
        bb.innings_no,
        bb.over_id,
        SUM(bs.runs_scored) AS batsman_runs,
        SUM(er.extra_runs) AS extra_runs,
        SUM(bs.runs_scored) + SUM(er.extra_runs) AS total_runs,
        MAX(bb.bowler_id) AS bowler_id -- Any ball's bowler for the over, since all should be the same
    FROM ball_by_ball bb
    LEFT JOIN batsman_scored bs
        ON bb.match_id = bs.match_id
        AND bb.innings_no = bs.innings_no
        AND bb.over_id = bs.over_id
        AND bb.ball_id = bs.ball_id
    LEFT JOIN extra_runs er
        ON bb.match_id = er.match_id
        AND bb.innings_no = er.innings_no
        AND bb.over_id = er.over_id
        AND bb.ball_id = er.ball_id
    GROUP BY
        bb.match_id,
        bb.innings_no,
        bb.over_id
),
max_over_per_match AS (
    SELECT
        match_id,
        innings_no,
        over_id,
        bowler_id,
        total_runs
    FROM (
        SELECT
            match_id,
            innings_no,
            over_id,
            bowler_id,
            total_runs,
            ROW_NUMBER() OVER (PARTITION BY match_id ORDER BY total_runs DESC, innings_no, over_id) AS rn
        FROM over_runs
    )
    WHERE rn = 1
)
SELECT
    AVG(total_runs * 1.0) AS average_highest_over_runs
FROM max_over_per_match;
