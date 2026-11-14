WITH season5_matches AS (
    SELECT match_id 
    FROM match 
    WHERE season = 5
),
player_runs AS (
    SELECT 
        pm.player_id,
        SUM(bs.runs_scored) AS total_runs,
        COUNT(DISTINCT pm.match_id) AS matches_played
    FROM player_match pm
    JOIN batsman_scored bs ON pm.match_id = bs.match_id AND pm.player_id = bs.batsman_id
    WHERE pm.match_id IN (SELECT match_id FROM season5_matches)
    GROUP BY pm.player_id
),
batting_averages AS (
    SELECT
        pr.player_id,
        pr.total_runs * 1.0 / NULLIF(pr.matches_played, 0) AS avg_runs_per_match
    FROM player_runs pr
)
SELECT 
    p.player_name,
    ba.avg_runs_per_match
FROM batting_averages ba
JOIN player p ON ba.player_id = p.player_id
ORDER BY ba.avg_runs_per_match DESC
LIMIT 5;
