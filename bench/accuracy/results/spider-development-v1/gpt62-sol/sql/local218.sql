WITH match_goals AS (
    SELECT season, home_team_api_id AS team_id, home_team_goal AS goals
    FROM "Match"
    UNION ALL
    SELECT season, away_team_api_id AS team_id, away_team_goal AS goals
    FROM "Match"
), season_totals AS (
    SELECT team_id, season, SUM(goals) AS season_goals
    FROM match_goals
    WHERE team_id IS NOT NULL AND season IS NOT NULL AND goals IS NOT NULL
    GROUP BY team_id, season
), team_best AS (
    SELECT team_id, MAX(season_goals) AS highest_season_goals
    FROM season_totals
    GROUP BY team_id
), ranked AS (
    SELECT highest_season_goals,
           ROW_NUMBER() OVER (ORDER BY highest_season_goals, team_id) AS rn,
           COUNT(*) OVER () AS team_count
    FROM team_best
)
SELECT AVG(highest_season_goals) AS median_highest_season_goals
FROM ranked
WHERE rn BETWEEN (team_count + 1) / 2 AND (team_count + 2) / 2;
