WITH salary_bounds AS (
    SELECT MIN(A11) AS lowest, MAX(A11) AS highest
    FROM main.district
),
eligible_clients AS (
    SELECT c.client_id, c.birth_date
    FROM main.client AS c
    JOIN main.district AS d ON d.district_id = c.district_id
    JOIN salary_bounds AS s ON d.A11 = s.lowest
    WHERE c.gender = 'F'
)
SELECT DISTINCT p.account_id, s.highest - s.lowest AS salary_difference
FROM eligible_clients AS c
JOIN main.disp AS p ON p.client_id = c.client_id
CROSS JOIN salary_bounds AS s
WHERE c.birth_date = (SELECT MIN(birth_date) FROM eligible_clients)
ORDER BY p.account_id
LIMIT 1000
