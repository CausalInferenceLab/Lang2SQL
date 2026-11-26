WITH appearances AS (
    SELECT league_id, home_team_api_id AS team_api_id,
           CASE WHEN home_team_goal > away_team_goal THEN 1 ELSE 0 END AS won
    FROM "Match"
    UNION ALL
    SELECT league_id, away_team_api_id AS team_api_id,
           CASE WHEN away_team_goal > home_team_goal THEN 1 ELSE 0 END AS won
    FROM "Match"
), win_totals AS (
    SELECT league_id, team_api_id, SUM(won) AS total_wins
    FROM appearances
    GROUP BY league_id, team_api_id
), ranked AS (
    SELECT w.league_id, t.team_long_name AS team_name, w.total_wins,
           ROW_NUMBER() OVER (
               PARTITION BY w.league_id
               ORDER BY w.total_wins, w.team_api_id, t.id
           ) AS league_rank
    FROM win_totals AS w
    JOIN Team AS t ON t.team_api_id = w.team_api_id
)
SELECT l.name AS league_name, r.team_name, r.total_wins
FROM ranked AS r
JOIN League AS l ON l.id = r.league_id
WHERE r.league_rank = 1
ORDER BY l.name, l.id;
