WITH passing_2012 AS (
    SELECT team_api_id, buildUpPlayPassing
    FROM Team_Attributes
    WHERE date >= '2012-01-01' AND date < '2013-01-01'
)
SELECT t.team_long_name
FROM Team AS t
JOIN passing_2012 AS p ON p.team_api_id = t.team_api_id
WHERE p.buildUpPlayPassing > (SELECT AVG(buildUpPlayPassing) FROM passing_2012)
ORDER BY t.team_long_name;
