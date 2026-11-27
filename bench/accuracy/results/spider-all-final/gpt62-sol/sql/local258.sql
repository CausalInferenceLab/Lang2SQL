WITH match_bowling AS (
    SELECT
        b.bowler,
        b.match_id,
        SUM(CASE WHEN w.kind_out IN (
            'bowled', 'caught', 'caught and bowled', 'lbw', 'stumped', 'hit wicket'
        ) THEN 1 ELSE 0 END) AS wickets,
        SUM(COALESCE(s.runs_scored, 0)) AS runs_conceded,
        SUM(CASE WHEN e.extra_type IN ('wides', 'noballs')
                 THEN 0 ELSE 1 END) AS legal_balls
    FROM ball_by_ball AS b
    LEFT JOIN batsman_scored AS s USING (match_id, innings_no, over_id, ball_id)
    LEFT JOIN wicket_taken AS w USING (match_id, innings_no, over_id, ball_id)
    LEFT JOIN extra_runs AS e USING (match_id, innings_no, over_id, ball_id)
    WHERE b.bowler IS NOT NULL
    GROUP BY b.bowler, b.match_id
), ranked AS (
    SELECT
        bowler,
        wickets,
        runs_conceded,
        SUM(wickets) OVER (PARTITION BY bowler) AS total_wickets,
        SUM(runs_conceded) OVER (PARTITION BY bowler) AS total_runs,
        SUM(legal_balls) OVER (PARTITION BY bowler) AS total_balls,
        ROW_NUMBER() OVER (
            PARTITION BY bowler
            ORDER BY wickets DESC, runs_conceded ASC, match_id ASC
        ) AS match_rank
    FROM match_bowling
)
SELECT
    COALESCE(p.player_name, CAST(r.bowler AS TEXT)) AS bowler,
    r.total_wickets,
    6.0 * r.total_runs / NULLIF(r.total_balls, 0) AS economy_rate,
    1.0 * r.total_balls / NULLIF(r.total_wickets, 0) AS strike_rate,
    r.wickets || '-' || r.runs_conceded AS best_bowling_performance
FROM ranked AS r
LEFT JOIN player AS p ON p.player_id = r.bowler
WHERE r.match_rank = 1
ORDER BY r.total_wickets DESC, bowler ASC;
