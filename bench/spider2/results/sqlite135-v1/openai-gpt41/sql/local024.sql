WITH player_runs AS (
    SELECT
        pm.player_id,
        p.country,
        SUM(bs.runs_scored) AS total_runs,
        COUNT(DISTINCT pm.match_id) AS matches_played
    FROM player_match pm
    JOIN player p ON pm.player_id = p.player_id
    LEFT JOIN batsman_scored bs ON bs.player_id = pm.player_id AND bs.match_id = pm.match_id
    GROUP BY pm.player_id, p.country
    HAVING matches_played > 0
),
player_avg AS (
    SELECT
        player_id,
        country,
        CAST(total_runs AS FLOAT) / matches_played AS avg_runs_per_match
    FROM player_runs
),
country_batting_averages AS (
    SELECT
        country,
        AVG(avg_runs_per_match) AS country_avg_batting
    FROM player_avg
    GROUP BY country
)
SELECT
    country,
    country_avg_batting
FROM country_batting_averages
ORDER BY country_avg_batting DESC
LIMIT 5;
