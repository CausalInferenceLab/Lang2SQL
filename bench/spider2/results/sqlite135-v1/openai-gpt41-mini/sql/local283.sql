WITH match_results AS (
  SELECT
    season,
    league_id,
    country_id,
    home_team_api_id AS team_api_id,
    CASE WHEN home_team_goal > away_team_goal THEN 3
         WHEN home_team_goal = away_team_goal THEN 1
         ELSE 0 END AS points
  FROM Match
  UNION ALL
  SELECT
    season,
    league_id,
    country_id,
    away_team_api_id AS team_api_id,
    CASE WHEN away_team_goal > home_team_goal THEN 3
         WHEN home_team_goal = away_team_goal THEN 1
         ELSE 0 END AS points
  FROM Match
),

team_points AS (
  SELECT
    season,
    league_id,
    country_id,
    team_api_id,
    SUM(points) AS total_points
  FROM match_results
  GROUP BY season, league_id, country_id, team_api_id
),

ranked_teams AS (
  SELECT
    season,
    league_id,
    country_id,
    team_api_id,
    total_points,
    RANK() OVER (PARTITION BY season, league_id, country_id ORDER BY total_points DESC) AS rank
  FROM team_points
)

SELECT
  t.team_long_name AS champion_team_name,
  l.name AS league_name,
  c.name AS country_name,
  rt.total_points
FROM ranked_teams rt
JOIN Team t ON rt.team_api_id = t.team_api_id
JOIN League l ON rt.league_id = l.id
JOIN Country c ON rt.country_id = c.id
WHERE rt.rank = 1
ORDER BY rt.season, c.name, l.name
