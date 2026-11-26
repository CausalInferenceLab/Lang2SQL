WITH maxima AS (
    SELECT MAX(g) AS games_played,
           MAX(CASE WHEN typeof(r) IN ('integer', 'real') THEN r END) AS runs,
           MAX(CASE WHEN typeof(h) IN ('integer', 'real') THEN h END) AS hits,
           MAX(CASE WHEN typeof(hr) IN ('integer', 'real') THEN hr END) AS home_runs
    FROM main.batting
),
leaders AS (
    SELECT 1 AS stat_order, 'Games played' AS statistic, b.player_id, b.g AS score_value
    FROM main.batting AS b
    JOIN maxima AS m ON b.g = m.games_played
    UNION ALL
    SELECT 2, 'Runs', b.player_id, b.r
    FROM main.batting AS b
    JOIN maxima AS m ON b.r = m.runs
    UNION ALL
    SELECT 3, 'Hits', b.player_id, b.h
    FROM main.batting AS b
    JOIN maxima AS m ON b.h = m.hits
    UNION ALL
    SELECT 4, 'Home runs', b.player_id, b.hr
    FROM main.batting AS b
    JOIN maxima AS m ON b.hr = m.home_runs
)
SELECT p.name_given, l.statistic, l.score_value
FROM leaders AS l
JOIN main.player AS p ON p.player_id = l.player_id
ORDER BY l.stat_order, p.name_given, l.player_id
LIMIT -1
