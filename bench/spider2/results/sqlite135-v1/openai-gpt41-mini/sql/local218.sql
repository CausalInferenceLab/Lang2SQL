WITH TeamSeasonGoals AS (
  SELECT
    team_api_id,
    season,
    MAX(goals) AS max_goals
  FROM Team_Attributes
  GROUP BY team_api_id, season
),
MaxGoalsPerTeam AS (
  SELECT
    team_api_id,
    MAX(max_goals) AS highest_season_goals
  FROM TeamSeasonGoals
  GROUP BY team_api_id
),
OrderedGoals AS (
  SELECT
    highest_season_goals,
    ROW_NUMBER() OVER (ORDER BY highest_season_goals) AS row_num,
    COUNT(*) OVER () AS total_count
  FROM MaxGoalsPerTeam
)
SELECT
  CASE
    WHEN total_count % 2 = 1 THEN
      (SELECT highest_season_goals FROM OrderedGoals WHERE row_num = (total_count + 1) / 2)
    ELSE
      ((SELECT highest_season_goals FROM OrderedGoals WHERE row_num = total_count / 2) +
       (SELECT highest_season_goals FROM OrderedGoals WHERE row_num = total_count / 2 + 1)) / 2.0
  END AS median_highest_season_goals
FROM OrderedGoals
LIMIT 1;
