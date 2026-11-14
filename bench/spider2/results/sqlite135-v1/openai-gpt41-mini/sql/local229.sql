WITH partnership_runs AS (
    SELECT
        match_id,
        batsman_id_1 AS player1,
        batsman_id_2 AS player2,
        SUM(batsman_1_runs + batsman_2_runs) AS partnership_run,
        SUM(batsman_1_runs) AS player1_runs,
        SUM(batsman_2_runs) AS player2_runs
    FROM (
        SELECT
            b.match_id,
            b.batsman AS batsman_id_1,
            LEAD(b.batsman) OVER (PARTITION BY b.match_id, b.inning ORDER BY b.ball_in_over, b.over) AS batsman_id_2,
            b.runs AS batsman_1_runs,
            LEAD(b.runs) OVER (PARTITION BY b.match_id, b.inning ORDER BY b.ball_in_over, b.over) AS batsman_2_runs
        FROM ball_by_ball b
    )
    WHERE batsman_id_2 IS NOT NULL
    GROUP BY match_id, player1, player2
),
max_partnerships AS (
    SELECT match_id, MAX(partnership_run) AS max_run
    FROM partnership_runs
    GROUP BY match_id
),
filtered_partnerships AS (
    SELECT p.*
    FROM partnership_runs p
    JOIN max_partnerships m ON p.match_id = m.match_id AND p.partnership_run = m.max_run
),
ordered_partnerships AS (
    SELECT
        match_id,
        CASE 
            WHEN player1_runs > player2_runs THEN player1
            WHEN player1_runs < player2_runs THEN player2
            ELSE CASE WHEN player1 > player2 THEN player1 ELSE player2 END
        END AS player1,
        CASE 
            WHEN player1_runs > player2_runs THEN player2
            WHEN player1_runs < player2_runs THEN player1
            ELSE CASE WHEN player1 > player2 THEN player2 ELSE player1 END
        END AS player2,
        CASE 
            WHEN player1_runs > player2_runs THEN player1_runs
            WHEN player1_runs < player2_runs THEN player2_runs
            ELSE CASE WHEN player1 > player2 THEN player1_runs ELSE player2_runs END
        END AS player1_runs,
        CASE 
            WHEN player1_runs > player2_runs THEN player2_runs
            WHEN player1_runs < player2_runs THEN player1_runs
            ELSE CASE WHEN player1 > player2 THEN player2_runs ELSE player1_runs END
        END AS player2_runs,
        partnership_run
    FROM filtered_partnerships
)
SELECT *
FROM ordered_partnerships
ORDER BY match_id, player1, player2;
