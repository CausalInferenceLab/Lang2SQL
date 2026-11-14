WITH partnership_runs AS (
    SELECT
        b1.match_id,
        b1.striker AS player1_id,
        b1.non_striker AS player2_id,
        SUM(b1.runs_scored) AS player1_runs,
        (
            SELECT COALESCE(SUM(b2.runs_scored), 0)
            FROM ball_by_ball b2
            WHERE b2.match_id = b1.match_id
              AND b2.striker = b1.non_striker
              AND b2.non_striker = b1.striker
        ) AS player2_runs,
        SUM(b1.runs_scored) + (
            SELECT COALESCE(SUM(b2.runs_scored), 0)
            FROM ball_by_ball b2
            WHERE b2.match_id = b1.match_id
              AND b2.striker = b1.non_striker
              AND b2.non_striker = b1.striker
        ) AS partnership_total
    FROM ball_by_ball b1
    GROUP BY b1.match_id, b1.striker, b1.non_striker
),
standardized_partnerships AS (
    SELECT
        match_id,
        CASE
            WHEN player1_runs > player2_runs THEN player1_id
            WHEN player1_runs < player2_runs THEN player2_id
            ELSE
                CASE WHEN player1_id > player2_id THEN player1_id ELSE player2_id END
        END AS player1_id,
        CASE
            WHEN player1_runs > player2_runs THEN player2_id
            WHEN player1_runs < player2_runs THEN player1_id
            ELSE
                CASE WHEN player1_id < player2_id THEN player1_id ELSE player2_id END
        END AS player2_id,
        CASE
            WHEN player1_runs > player2_runs THEN player1_runs
            WHEN player1_runs < player2_runs THEN player2_runs
            ELSE
                CASE WHEN player1_id > player2_id THEN player1_runs ELSE player2_runs END
        END AS player1_runs,
        CASE
            WHEN player1_runs > player2_runs THEN player2_runs
            WHEN player1_runs < player2_runs THEN player1_runs
            ELSE
                CASE WHEN player1_id < player2_id THEN player1_runs ELSE player2_runs END
        END AS player2_runs,
        partnership_total
    FROM partnership_runs
    WHERE player1_id <> player2_id
),
max_partnership_per_match AS (
    SELECT
        match_id,
        MAX(partnership_total) AS max_partnership
    FROM standardized_partnerships
    GROUP BY match_id
)
SELECT
    s.match_id,
    s.player1_id,
    s.player1_runs,
    s.player2_id,
    s.player2_runs,
    s.partnership_total
FROM standardized_partnerships s
JOIN max_partnership_per_match m
    ON s.match_id = m.match_id
   AND s.partnership_total = m.max_partnership
ORDER BY s.match_id, s.player1_id, s.player2_id;
