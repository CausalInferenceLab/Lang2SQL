WITH RECURSIVE
accent_map(accented, plain) AS (
    VALUES ('áčďéěíňóřšťúůýžÁČĎÉĚÍŇÓŘŠŤÚŮÝŽ', 'acdeeinorstuuyzACDEEINORSTUUYZ')
),
name_keys(district_id, sort_name, pos) AS (
    SELECT district_id,
           replace(replace(replace(A2, char(769), ''), char(778), ''), char(780), ''),
           1
    FROM main.district
    WHERE A3 = 'central Bohemia'
    UNION ALL
    SELECT district_id,
           replace(sort_name, substr(accented, pos, 1), substr(plain, pos, 1)),
           pos + 1
    FROM name_keys
    CROSS JOIN accent_map
    WHERE pos <= length(accented)
),
top_three AS (
    SELECT d.A2 AS district_name, d.A16 AS crimes_1996,
           CAST(d.A4 AS INTEGER) AS inhabitants, k.sort_name
    FROM name_keys AS k
    JOIN main.district AS d ON d.district_id = k.district_id
    WHERE k.pos = (SELECT length(accented) + 1 FROM accent_map)
    ORDER BY inhabitants DESC, k.sort_name COLLATE NOCASE
    LIMIT 3
),
averages AS (
    SELECT (SELECT AVG(crimes_1996) FROM top_three) AS top_three_avg,
           (SELECT AVG(A16) FROM main.district) AS overall_avg
)
SELECT t.district_name,
       t.crimes_1996,
       ROUND(a.top_three_avg, 2) AS top_three_avg_crimes_1996,
       ROUND(a.overall_avg, 2) AS overall_avg_crimes_1996,
       ROUND(a.top_three_avg - a.overall_avg, 2) AS difference_1996
FROM top_three AS t
CROSS JOIN averages AS a
ORDER BY t.inhabitants DESC, t.sort_name COLLATE NOCASE
LIMIT 3
