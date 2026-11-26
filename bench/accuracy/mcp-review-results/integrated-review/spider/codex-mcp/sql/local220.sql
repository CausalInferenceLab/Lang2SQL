WITH team_lineups AS (
    SELECT match_api_id,
           CASE WHEN home_team_goal > away_team_goal THEN 'Win' ELSE 'Loss' END AS outcome,
           json_array(
               home_player_1, home_player_2, home_player_3, home_player_4,
               home_player_5, home_player_6, home_player_7, home_player_8,
               home_player_9, home_player_10, home_player_11
           ) AS lineup
    FROM main."Match"
    WHERE home_team_goal <> away_team_goal

    UNION ALL

    SELECT match_api_id,
           CASE WHEN away_team_goal > home_team_goal THEN 'Win' ELSE 'Loss' END AS outcome,
           json_array(
               away_player_1, away_player_2, away_player_3, away_player_4,
               away_player_5, away_player_6, away_player_7, away_player_8,
               away_player_9, away_player_10, away_player_11
           ) AS lineup
    FROM main."Match"
    WHERE home_team_goal <> away_team_goal
), player_totals AS (
    SELECT l.outcome,
           j.value AS player_api_id,
           COUNT(DISTINCT l.match_api_id) AS match_count
    FROM team_lineups AS l
    CROSS JOIN json_each(l.lineup) AS j
    WHERE j.value IS NOT NULL
    GROUP BY l.outcome, j.value
), ranked AS (
    SELECT outcome, player_api_id, match_count,
           RANK() OVER (PARTITION BY outcome ORDER BY match_count DESC) AS outcome_rank
    FROM player_totals
)
SELECT r.outcome, p.player_name, r.match_count
FROM ranked AS r
JOIN main.Player AS p ON p.player_api_id = r.player_api_id
WHERE r.outcome_rank = 1
ORDER BY CASE r.outcome WHEN 'Win' THEN 1 ELSE 2 END, p.player_name
LIMIT 1000
