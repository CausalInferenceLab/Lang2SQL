WITH match_players(country_id, player_api_id) AS (
    SELECT country_id, home_player_1 FROM "Match"
    UNION ALL SELECT country_id, home_player_2 FROM "Match"
    UNION ALL SELECT country_id, home_player_3 FROM "Match"
    UNION ALL SELECT country_id, home_player_4 FROM "Match"
    UNION ALL SELECT country_id, home_player_5 FROM "Match"
    UNION ALL SELECT country_id, home_player_6 FROM "Match"
    UNION ALL SELECT country_id, home_player_7 FROM "Match"
    UNION ALL SELECT country_id, home_player_8 FROM "Match"
    UNION ALL SELECT country_id, home_player_9 FROM "Match"
    UNION ALL SELECT country_id, home_player_10 FROM "Match"
    UNION ALL SELECT country_id, home_player_11 FROM "Match"
    UNION ALL SELECT country_id, away_player_1 FROM "Match"
    UNION ALL SELECT country_id, away_player_2 FROM "Match"
    UNION ALL SELECT country_id, away_player_3 FROM "Match"
    UNION ALL SELECT country_id, away_player_4 FROM "Match"
    UNION ALL SELECT country_id, away_player_5 FROM "Match"
    UNION ALL SELECT country_id, away_player_6 FROM "Match"
    UNION ALL SELECT country_id, away_player_7 FROM "Match"
    UNION ALL SELECT country_id, away_player_8 FROM "Match"
    UNION ALL SELECT country_id, away_player_9 FROM "Match"
    UNION ALL SELECT country_id, away_player_10 FROM "Match"
    UNION ALL SELECT country_id, away_player_11 FROM "Match"
), country_counts AS (
    SELECT c.name AS country_name, COUNT(DISTINCT p.player_api_id) AS player_count
    FROM Country AS c
    JOIN match_players AS mp ON mp.country_id = c.id
    LEFT JOIN Player AS p ON p.player_api_id = mp.player_api_id AND p.weight <= 160
    GROUP BY c.id, c.name
)
SELECT country_name
FROM country_counts
WHERE player_count = (SELECT MIN(player_count) FROM country_counts)
ORDER BY country_name;
