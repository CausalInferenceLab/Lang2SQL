WITH player_stint_highs AS (
    SELECT
        player_id,
        MAX(g) AS games_played,
        MAX(NULLIF(r, '')) AS runs,
        MAX(NULLIF(h, '')) AS hits,
        MAX(NULLIF(hr, '')) AS home_runs
    FROM main.batting
    GROUP BY player_id
),
scores (player_id, stat_order, statistic, score) AS (
    SELECT player_id, 1, 'Games played', games_played FROM player_stint_highs
    UNION ALL
    SELECT player_id, 2, 'Runs', runs FROM player_stint_highs
    UNION ALL
    SELECT player_id, 3, 'Hits', hits FROM player_stint_highs
    UNION ALL
    SELECT player_id, 4, 'Home runs', home_runs FROM player_stint_highs
),
ranked AS (
    SELECT scores.*, MAX(score) OVER (PARTITION BY statistic) AS record_score
    FROM scores
)
SELECT r.statistic, p.name_given, r.score
FROM ranked AS r
JOIN main.player AS p ON p.player_id = r.player_id
WHERE r.score = r.record_score
ORDER BY r.stat_order, p.name_given
LIMIT -1
