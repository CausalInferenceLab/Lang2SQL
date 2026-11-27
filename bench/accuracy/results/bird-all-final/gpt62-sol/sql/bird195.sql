SELECT d.A2 AS district
FROM client AS c
JOIN disp AS p ON p.client_id = c.client_id
JOIN account AS a ON a.account_id = p.account_id
JOIN district AS d ON d.district_id = a.district_id
WHERE c.gender = 'F'
  AND c.birth_date = '1976-01-29'
  AND p.type = 'OWNER';
