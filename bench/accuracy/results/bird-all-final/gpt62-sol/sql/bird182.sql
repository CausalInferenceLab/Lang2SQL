WITH heavy_away_defeats AS (
    SELECT season, away_player_1, away_player_2, away_player_3,
           away_player_4, away_player_5, away_player_6, away_player_7,
           away_player_8, away_player_9, away_player_10, away_player_11
    FROM "Match"
    WHERE home_team_goal - away_team_goal >= 5
), away_lineup (season, player_id) AS (
    SELECT season, away_player_1 FROM heavy_away_defeats
    UNION ALL SELECT season, away_player_2 FROM heavy_away_defeats
    UNION ALL SELECT season, away_player_3 FROM heavy_away_defeats
    UNION ALL SELECT season, away_player_4 FROM heavy_away_defeats
    UNION ALL SELECT season, away_player_5 FROM heavy_away_defeats
    UNION ALL SELECT season, away_player_6 FROM heavy_away_defeats
    UNION ALL SELECT season, away_player_7 FROM heavy_away_defeats
    UNION ALL SELECT season, away_player_8 FROM heavy_away_defeats
    UNION ALL SELECT season, away_player_9 FROM heavy_away_defeats
    UNION ALL SELECT season, away_player_10 FROM heavy_away_defeats
    UNION ALL SELECT season, away_player_11 FROM heavy_away_defeats
)
SELECT season, COUNT(DISTINCT player_id) AS player_count
FROM away_lineup
GROUP BY season
ORDER BY season;
