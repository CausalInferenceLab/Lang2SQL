WITH over_runs AS (
    SELECT
        bb.match_id,
        bb.inning,
        bb.over_id,
        bb.bowler,
        SUM(bs.runs_scored) + COALESCE(SUM(er.extra_runs),0) AS total_runs
    FROM
        ball_by_ball bb
        LEFT JOIN batsman_scored bs
            ON bb.match_id = bs.match_id AND bb.inning = bs.inning AND bb.over_id = bs.over_id AND bb.ball_id = bs.ball_id
        LEFT JOIN extra_runs er
            ON bb.match_id = er.match_id AND bb.inning = er.inning AND bb.over_id = er.over_id AND bb.ball_id = er.ball_id
    GROUP BY
        bb.match_id, bb.inning, bb.over_id, bb.bowler
),
max_runs_per_match AS (
    SELECT
        match_id,
        MAX(total_runs) AS max_over_runs
    FROM
        over_runs
    GROUP BY
        match_id
),
top_bowler_overs AS (
    SELECT
        o.bowler,
        o.match_id,
        o.inning,
        o.over_id,
        o.total_runs
    FROM
        over_runs o
        INNER JOIN max_runs_per_match m
            ON o.match_id = m.match_id AND o.total_runs = m.max_over_runs
)
SELECT
    p.player_name AS bowler_name,
    tbo.match_id,
    tbo.total_runs AS runs_conceded_in_over
FROM
    top_bowler_overs tbo
    INNER JOIN player p ON tbo.bowler = p.player_id
ORDER BY
    tbo.total_runs DESC
LIMIT 3;
