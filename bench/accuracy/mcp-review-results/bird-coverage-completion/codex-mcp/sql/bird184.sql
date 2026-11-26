WITH season_players AS (
    SELECT DISTINCT m.season, slot.value AS player_api_id
    FROM main."Match" AS m
    JOIN main.League AS l ON l.id = m.league_id
    JOIN main.Country AS c ON c.id = l.country_id
    CROSS JOIN json_each(json_array(
        m.home_player_1, m.home_player_2, m.home_player_3, m.home_player_4,
        m.home_player_5, m.home_player_6, m.home_player_7, m.home_player_8,
        m.home_player_9, m.home_player_10, m.home_player_11,
        m.away_player_1, m.away_player_2, m.away_player_3, m.away_player_4,
        m.away_player_5, m.away_player_6, m.away_player_7, m.away_player_8,
        m.away_player_9, m.away_player_10, m.away_player_11
    )) AS slot
    WHERE c.name = 'Italy'
      AND m.season IN ('2008/2009', '2015/2016')
      AND slot.value IS NOT NULL
)
SELECT sp.season,
       COUNT(*) AS player_count,
       ROUND(AVG(p.height), 2) AS average_height_cm
FROM season_players AS sp
LEFT JOIN main.Player AS p ON p.player_api_id = sp.player_api_id
GROUP BY sp.season
ORDER BY sp.season
LIMIT 2
