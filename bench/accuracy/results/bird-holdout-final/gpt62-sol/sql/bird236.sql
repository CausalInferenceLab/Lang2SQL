SELECT ROUND(AVG(d.A11), 2) AS average_salary
FROM district AS d
WHERE EXISTS (
    SELECT 1
    FROM account AS a
    JOIN disp AS dp ON dp.account_id = a.account_id
    JOIN client AS c ON c.client_id = dp.client_id
    WHERE a.district_id = d.district_id
      AND c.gender = 'F'
      AND DATE(c.birth_date) < '1930-01-01'
);
