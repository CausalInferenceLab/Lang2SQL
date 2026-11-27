WITH RECURSIVE
accent_map(lower_chars, upper_chars, plain_chars) AS (
    VALUES ('áčďéěíňóřšťúůýž', 'ÁČĎÉĚÍŇÓŘŠŤÚŮÝŽ', 'acdeeinorstuuyz')
),
central_names(district_name, inhabitants, crimes_1996, sort_name, n) AS (
    SELECT A2, CAST(A4 AS INTEGER), A16, lower(A2), 0
    FROM district
    WHERE A3 = 'central Bohemia'
    UNION ALL
    SELECT district_name, inhabitants, crimes_1996,
           replace(
               replace(sort_name, substr(lower_chars, n + 1, 1), substr(plain_chars, n + 1, 1)),
               substr(upper_chars, n + 1, 1), substr(plain_chars, n + 1, 1)
           ),
           n + 1
    FROM central_names CROSS JOIN accent_map
    WHERE n < length(plain_chars)
),
top_three AS (
    SELECT district_name, inhabitants, crimes_1996, sort_name
    FROM central_names CROSS JOIN accent_map
    WHERE n = length(plain_chars)
    ORDER BY inhabitants DESC, sort_name ASC
    LIMIT 3
),
averages AS (
    SELECT (SELECT AVG(crimes_1996) FROM top_three) AS top_three_avg,
           AVG(A16) AS overall_avg
    FROM district
)
SELECT district_name,
       crimes_1996,
       ROUND(top_three_avg, 2) AS top_three_avg_1996,
       ROUND(overall_avg, 2) AS overall_avg_1996,
       ROUND(top_three_avg - overall_avg, 2) AS difference_1996
FROM top_three CROSS JOIN averages
ORDER BY inhabitants DESC, sort_name ASC;
