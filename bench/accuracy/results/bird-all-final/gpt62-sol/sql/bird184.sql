WITH italian_matches AS (
    SELECT m.*
    FROM "Match" AS m
    JOIN League AS l ON l.id = m.league_id
    JOIN Country AS c ON c.id = l.country_id
    WHERE c.name = 'Italy'
      AND m.season IN ('2008/2009', '2015/2016')
), season_players AS (
    SELECT season, home_player_1 AS player_api_id FROM italian_matches
    UNION SELECT season, home_player_2 FROM italian_matches
    UNION SELECT season, home_player_3 FROM italian_matches
    UNION SELECT season, home_player_4 FROM italian_matches
    UNION SELECT season, home_player_5 FROM italian_matches
    UNION SELECT season, home_player_6 FROM italian_matches
    UNION SELECT season, home_player_7 FROM italian_matches
    UNION SELECT season, home_player_8 FROM italian_matches
    UNION SELECT season, home_player_9 FROM italian_matches
    UNION SELECT season, home_player_10 FROM italian_matches
    UNION SELECT season, home_player_11 FROM italian_matches
    UNION SELECT season, away_player_1 FROM italian_matches
    UNION SELECT season, away_player_2 FROM italian_matches
    UNION SELECT season, away_player_3 FROM italian_matches
    UNION SELECT season, away_player_4 FROM italian_matches
    UNION SELECT season, away_player_5 FROM italian_matches
    UNION SELECT season, away_player_6 FROM italian_matches
    UNION SELECT season, away_player_7 FROM italian_matches
    UNION SELECT season, away_player_8 FROM italian_matches
    UNION SELECT season, away_player_9 FROM italian_matches
    UNION SELECT season, away_player_10 FROM italian_matches
    UNION SELECT season, away_player_11 FROM italian_matches
)
SELECT sp.season,
       COUNT(sp.player_api_id) AS player_count,
       ROUND(AVG(p.height), 2) AS average_height_cm
FROM season_players AS sp
LEFT JOIN Player AS p ON p.player_api_id = sp.player_api_id
GROUP BY sp.season
ORDER BY sp.season;
