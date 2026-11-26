WITH maxima AS (
    SELECT MAX(g) AS max_g,
           MAX(NULLIF(r, '')) AS max_r,
           MAX(NULLIF(h, '')) AS max_h,
           MAX(NULLIF(hr, '')) AS max_hr
    FROM batting
), leaders AS (
    SELECT 'games played' AS statistic, b.player_id, b.g AS score
    FROM batting AS b JOIN maxima AS m ON b.g = m.max_g
    UNION
    SELECT 'runs', b.player_id, b.r
    FROM batting AS b JOIN maxima AS m ON b.r = m.max_r
    UNION
    SELECT 'hits', b.player_id, b.h
    FROM batting AS b JOIN maxima AS m ON b.h = m.max_h
    UNION
    SELECT 'home runs', b.player_id, b.hr
    FROM batting AS b JOIN maxima AS m ON b.hr = m.max_hr
)
SELECT p.name_given, l.statistic, l.score
FROM leaders AS l
JOIN player AS p ON p.player_id = l.player_id
ORDER BY CASE l.statistic
    WHEN 'games played' THEN 1
    WHEN 'runs' THEN 2
    WHEN 'hits' THEN 3
    WHEN 'home runs' THEN 4
END, p.name_given;
