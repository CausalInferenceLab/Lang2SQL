WITH player_match_team AS (
    SELECT
        pm.player_api_id,
        m.id AS match_id,
        CASE
            WHEN pm.player_role = 'home' THEN m.home_team_api_id
            ELSE m.away_team_api_id
        END AS team_api_id,
        CASE
            WHEN pm.player_role = 'home' THEN m.away_team_api_id
            ELSE m.home_team_api_id
        END AS opponent_team_api_id,
        CASE
            WHEN pm.player_role = 'home' THEN m.home_team_goal
            ELSE m.away_team_goal
        END AS team_goal,
        CASE
            WHEN pm.player_role = 'home' THEN m.away_team_goal
            ELSE m.home_team_goal
        END AS opponent_goal
    FROM (
        SELECT
            id AS match_id, 
            player1 AS player_api_id, 'home' AS player_role FROM Match WHERE player1 IS NOT NULL
        UNION ALL SELECT id, player2, 'home' FROM Match WHERE player2 IS NOT NULL
        UNION ALL SELECT id, player3, 'home' FROM Match WHERE player3 IS NOT NULL
        UNION ALL SELECT id, player4, 'home' FROM Match WHERE player4 IS NOT NULL
        UNION ALL SELECT id, player5, 'home' FROM Match WHERE player5 IS NOT NULL
        UNION ALL SELECT id, player6, 'home' FROM Match WHERE player6 IS NOT NULL
        UNION ALL SELECT id, player7, 'home' FROM Match WHERE player7 IS NOT NULL
        UNION ALL SELECT id, player8, 'home' FROM Match WHERE player8 IS NOT NULL
        UNION ALL SELECT id, player9, 'home' FROM Match WHERE player9 IS NOT NULL
        UNION ALL SELECT id, player10, 'home' FROM Match WHERE player10 IS NOT NULL
        UNION ALL SELECT id, player11, 'home' FROM Match WHERE player11 IS NOT NULL
        UNION ALL SELECT id, player1, 'away' FROM Match WHERE player1 IS NOT NULL
        UNION ALL SELECT id, player2, 'away' FROM Match WHERE player2 IS NOT NULL
        UNION ALL SELECT id, player3, 'away' FROM Match WHERE player3 IS NOT NULL
        UNION ALL SELECT id, player4, 'away' FROM Match WHERE player4 IS NOT NULL
        UNION ALL SELECT id, player5, 'away' FROM Match WHERE player5 IS NOT NULL
        UNION ALL SELECT id, player6, 'away' FROM Match WHERE player6 IS NOT NULL
        UNION ALL SELECT id, player7, 'away' FROM Match WHERE player7 IS NOT NULL
        UNION ALL SELECT id, player8, 'away' FROM Match WHERE player8 IS NOT NULL
        UNION ALL SELECT id, player9, 'away' FROM Match WHERE player9 IS NOT NULL
        UNION ALL SELECT id, player10, 'away' FROM Match WHERE player10 IS NOT NULL
        UNION ALL SELECT id, player11, 'away' FROM Match WHERE player11 IS NOT NULL
    ) pm
    JOIN Match m ON pm.match_id = m.id
    WHERE 
        -- Actually played: non-null player columns (already ensured above)
        1=1
),
outcomes AS (
    SELECT
        pmt.player_api_id,
        pmt.match_id,
        CASE
            WHEN pmt.team_goal > pmt.opponent_goal THEN 1
            WHEN pmt.team_goal < pmt.opponent_goal THEN -1
            ELSE 0
        END AS result
    FROM player_match_team pmt
),
player_win_loss AS (
    SELECT
        player_api_id,
        SUM(CASE WHEN result = 1 THEN 1 ELSE 0 END) AS win_matches,
        SUM(CASE WHEN result = -1 THEN 1 ELSE 0 END) AS loss_matches
    FROM outcomes
    WHERE result != 0 -- exclude draws
    GROUP BY player_api_id
),
max_counts AS (
    SELECT
        (SELECT MAX(win_matches) FROM player_win_loss) AS max_win_matches,
        (SELECT MAX(loss_matches) FROM player_win_loss) AS max_loss_matches
),
players_with_names AS (
    SELECT
        p.player_api_id,
        pl.player_name,
        p.win_matches,
        p.loss_matches
    FROM player_win_loss p
    JOIN Player pl ON pl.player_api_id = p.player_api_id
)
SELECT 
    'win' AS category, player_name, win_matches AS match_count
FROM players_with_names, max_counts
WHERE win_matches = max_win_matches
UNION ALL
SELECT 
    'loss' AS category, player_name, loss_matches AS match_count
FROM players_with_names, max_counts
WHERE loss_matches = max_loss_matches
ORDER BY category;
