WITH all_wins AS (
  SELECT
    m.league_id,
    m.home_team_api_id AS team_api_id,
    SUM(CASE WHEN m.home_team_goal > m.away_team_goal THEN 1 ELSE 0 END) AS wins
  FROM Match m
  GROUP BY m.league_id, m.home_team_api_id
  UNION ALL
  SELECT
    m.league_id,
    m.away_team_api_id AS team_api_id,
    SUM(CASE WHEN m.away_team_goal > m.home_team_goal THEN 1 ELSE 0 END) AS wins
  FROM Match m
  GROUP BY m.league_id, m.away_team_api_id
),
team_league_combos AS (
  SELECT DISTINCT l.id AS league_id, t.team_api_id
  FROM League l CROSS JOIN Team t
),
total_wins AS (
  SELECT
    c.league_id,
    c.team_api_id,
    COALESCE(SUM(aw.wins), 0) AS total_wins
  FROM team_league_combos c
  LEFT JOIN all_wins aw
    ON c.league_id = aw.league_id AND c.team_api_id = aw.team_api_id
  GROUP BY c.league_id, c.team_api_id
),
ranked_wins AS (
  SELECT
    tw.league_id,
    tw.team_api_id,
    tw.total_wins,
    ROW_NUMBER() OVER (PARTITION BY tw.league_id ORDER BY tw.total_wins ASC, tw.team_api_id) AS rn
  FROM total_wins tw
)
SELECT
  l.name AS league_name,
  tt.team_long_name,
  rw.total_wins
FROM ranked_wins rw
JOIN League l ON rw.league_id = l.id
JOIN Team tt ON rw.team_api_id = tt.team_api_id
WHERE rw.rn = 1
ORDER BY l.name;
