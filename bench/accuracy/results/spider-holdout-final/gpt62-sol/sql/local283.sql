WITH match_points AS (
    SELECT season, league_id, country_id, home_team_api_id AS team_api_id,
           CASE
               WHEN home_team_goal > away_team_goal THEN 3
               WHEN home_team_goal = away_team_goal THEN 1
               ELSE 0
           END AS points
    FROM Match
    UNION ALL
    SELECT season, league_id, country_id, away_team_api_id AS team_api_id,
           CASE
               WHEN away_team_goal > home_team_goal THEN 3
               WHEN away_team_goal = home_team_goal THEN 1
               ELSE 0
           END AS points
    FROM Match
), season_totals AS (
    SELECT season, league_id, country_id, team_api_id, SUM(points) AS total_points
    FROM match_points
    GROUP BY season, league_id, country_id, team_api_id
), ranked AS (
    SELECT season, league_id, country_id, team_api_id, total_points,
           RANK() OVER (PARTITION BY season ORDER BY total_points DESC) AS points_rank
    FROM season_totals
)
SELECT r.season,
       t.team_long_name AS team_name,
       l.name AS league,
       c.name AS country,
       r.total_points
FROM ranked AS r
JOIN Team AS t ON t.team_api_id = r.team_api_id
JOIN League AS l ON l.id = r.league_id
JOIN Country AS c ON c.id = r.country_id
WHERE r.points_rank = 1
ORDER BY r.season, league, country, team_name;
