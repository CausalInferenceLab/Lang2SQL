WITH match_max_runs_per_over AS (
    SELECT
        match_id,
        inning,
        over,
        SUM(runs) AS runs_in_over
    FROM ball_by_ball
    GROUP BY match_id, inning, over
),
match_max_over_runs AS (
    SELECT
        match_id,
        MAX(runs_in_over) AS max_runs_in_over
    FROM match_max_runs_per_over
    GROUP BY match_id
),
max_run_overs AS (
    SELECT
        mro.match_id,
        mro.inning,
        mro.over,
        mro.runs_in_over
    FROM match_max_runs_per_over mro
    JOIN match_max_over_runs mmor ON mro.match_id = mmor.match_id AND mro.runs_in_over = mmor.max_runs_in_over
),
bowler_runs_in_max_overs AS (
    SELECT
        mmo.match_id,
        pmb.player_id AS bowler_id,
        SUM(b.runs) AS runs_conceded_in_max_over
    FROM max_run_overs mmo
    JOIN ball_by_ball b ON b.match_id = mmo.match_id AND b.inning = mmo.inning AND b.over = mmo.over
    JOIN player_match pmb ON pmb.match_id = b.match_id AND pmb.player_id = b.bowler_id AND pmb.player_id = b.bowler_id
    GROUP BY mmo.match_id, pmb.player_id, mmo.inning, mmo.over
),
bowler_max_over_runs AS (
    SELECT
        bowler_id,
        match_id,
        MAX(runs_conceded_in_max_over) AS max_runs_in_single_max_over
    FROM bowler_runs_in_max_overs
    GROUP BY bowler_id, match_id
),
top_bowlers AS (
    SELECT
        bowler_id,
        match_id,
        max_runs_in_single_max_over
    FROM bowler_max_over_runs
    ORDER BY max_runs_in_single_max_over DESC
    LIMIT 3
)
SELECT
    b.player_name,
    tb.match_id,
    tb.max_runs_in_single_max_over
FROM top_bowlers tb
JOIN player b ON tb.bowler_id = b.player_id
ORDER BY tb.max_runs_in_single_max_over DESC
LIMIT 3;
