WITH team_match_goals AS (
    SELECT home_team_api_id AS team_id, season, home_team_goal AS goals
    FROM "Match"
    UNION ALL
    SELECT away_team_api_id AS team_id, season, away_team_goal AS goals
    FROM "Match"
),
season_totals AS (
    SELECT team_id, season, SUM(goals) AS season_goals
    FROM team_match_goals
    GROUP BY team_id, season
),
team_peaks AS (
    SELECT team_id, MAX(season_goals) AS peak_goals
    FROM season_totals
    GROUP BY team_id
),
ranked AS (
    SELECT peak_goals,
           ROW_NUMBER() OVER (ORDER BY peak_goals) AS pos,
           COUNT(*) OVER () AS n
    FROM team_peaks
    WHERE peak_goals IS NOT NULL
)
SELECT AVG(peak_goals) AS median_highest_season_goals
FROM ranked
WHERE pos IN ((n + 1) / 2, (n + 2) / 2);
