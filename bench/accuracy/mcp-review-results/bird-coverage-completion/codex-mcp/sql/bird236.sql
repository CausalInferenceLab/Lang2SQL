SELECT ROUND(AVG(d.A11), 2) AS average_salary
FROM main.district AS d
WHERE d.district_id IN (
    SELECT a.district_id
    FROM main.client AS c
    JOIN main.disp AS p ON p.client_id = c.client_id
    JOIN main.account AS a ON a.account_id = p.account_id
    WHERE c.gender = 'F'
      AND c.birth_date < '1930-01-01'
)
LIMIT 1
