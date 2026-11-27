WITH salary_bounds AS (
    SELECT MIN(A11) AS lowest_salary, MAX(A11) AS highest_salary
    FROM district
), eligible_clients AS (
    SELECT c.client_id, c.birth_date
    FROM client AS c
    JOIN district AS d ON d.district_id = c.district_id
    CROSS JOIN salary_bounds AS s
    WHERE c.gender = 'F' AND d.A11 = s.lowest_salary
)
SELECT DISTINCT p.account_id,
       s.highest_salary - s.lowest_salary AS salary_difference
FROM eligible_clients AS c
JOIN disp AS p ON p.client_id = c.client_id
CROSS JOIN salary_bounds AS s
WHERE c.birth_date = (SELECT MIN(birth_date) FROM eligible_clients)
ORDER BY p.account_id;
