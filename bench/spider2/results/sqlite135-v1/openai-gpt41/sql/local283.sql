WITH match_points AS (
  SELECT
    M.season,
    L.name AS league_name,
    C.name AS country_name,
    CASE
      WHEN M.home_team_goal > M.away_team_goal THEN M.home_team_api_id
      WHEN M.home_team_goal < M.away_team_goal THEN M.away_team_api_id
      ELSE NULL
    END AS winner_team_id,
    CASE
      WHEN M.home_team_goal > M.away_team_goal THEN M.away_team_api_id
      WHEN M.home_team_goal < M.away_team_goal THEN M.home_team_api_id
      ELSE NULL
    END AS loser_team_id,
    CASE
      WHEN M.home_team_goal = M.away_team_goal THEN M.home_team_api_id
      ELSE NULL
    END AS tie_team1_id,
    CASE
      WHEN M.home_team_goal = M.away_team_goal THEN M.away_team_api_id
      ELSE NULL
    END AS tie_team2_id
  FROM Match M
  JOIN League L ON M.league_id = L.id
  JOIN Country C ON L.country_id = C.id
),
points_by_team AS (
  SELECT
    season,
    league_name,
    country_name,
    winner_team_id AS team_id,
    3 AS points
  FROM match_points
  WHERE winner_team_id IS NOT NULL

  UNION ALL

  SELECT
    season,
    league_name,
    country_name,
    tie_team1_id AS team_id,
    1 AS points
  FROM match_points
  WHERE tie_team1_id IS NOT NULL

  UNION ALL

  SELECT
    season,
    league_name,
    country_name,
    tie_team2_id AS team_id,
    1 AS points
  FROM match_points
  WHERE tie_team2_id IS NOT NULL
),
season_team_points AS (
  SELECT
    season,
    league_name,
    country_name,
    team_id,
    SUM(points) AS total_points
  FROM points_by_team
  GROUP BY season, league_name, country_name, team_id
),
champion_by_season AS (
  SELECT
    stp.season,
    stp.league_name,
    stp.country_name,
    stp.team_id,
    stp.total_points,
    ROW_NUMBER() OVER (
      PARTITION BY stp.country_name, stp.league_name, stp.season
      ORDER BY stp.total_points DESC, stp.team_id
    ) AS rn
  FROM season_team_points stp
)
SELECT
  cbs.season,
  cbs.country_name,
  cbs.league_name,
  T.team_long_name AS champion_team_name,
  cbs.total_points
FROM champion_by_season cbs
JOIN Team T ON cbs.team_id = T.team_api_id
WHERE cbs.rn = 1
ORDER BY cbs.country_name, cbs.league_name, cbs.season;
