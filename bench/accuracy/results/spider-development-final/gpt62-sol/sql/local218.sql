WITH match_goals AS (
    SELECT season, home_team_api_id AS team_api_id, home_team_goal AS goals
    FROM "Match"
    UNION ALL
    SELECT season, away_team_api_id AS team_api_id, away_team_goal AS goals
    FROM "Match"
), season_totals AS (
    SELECT team_api_id, season, SUM(goals) AS season_goals
    FROM match_goals
    GROUP BY team_api_id, season
), team_peaks AS (
    SELECT team_api_id, MAX(season_goals) AS highest_season_goals
    FROM season_totals
    GROUP BY team_api_id
), ranked AS (
    SELECT highest_season_goals,
           ROW_NUMBER() OVER (ORDER BY highest_season_goals) AS rn,
           COUNT(*) OVER () AS n
    FROM team_peaks
)
SELECT AVG(highest_season_goals) AS median_highest_season_goals
FROM ranked
WHERE rn IN ((n + 1) / 2, (n + 2) / 2);
