WITH TeamSeasonGoals AS (
  SELECT
    t.team_api_id,
    strftime('%Y', m.date) AS season,
    SUM(
      CASE 
        WHEN t.team_api_id = m.home_team_api_id THEN m.home_team_goal
        WHEN t.team_api_id = m.away_team_api_id THEN m.away_team_goal
        ELSE 0
      END
    ) AS total_goals
  FROM Team t
  JOIN Match m
    ON t.team_api_id = m.home_team_api_id OR t.team_api_id = m.away_team_api_id
  GROUP BY t.team_api_id, strftime('%Y', m.date)
),
TeamMaxSeasonGoals AS (
  SELECT
    team_api_id,
    MAX(total_goals) AS max_season_goals
  FROM TeamSeasonGoals
  GROUP BY team_api_id
),
OrderedGoals AS (
  SELECT
    max_season_goals,
    ROW_NUMBER() OVER (ORDER BY max_season_goals) AS rn,
    COUNT(*) OVER () AS cnt
  FROM TeamMaxSeasonGoals
)
SELECT
  AVG(max_season_goals) AS median_of_highest_season_goals
FROM OrderedGoals
WHERE rn IN ((cnt + 1) / 2, (cnt + 2) / 2);
