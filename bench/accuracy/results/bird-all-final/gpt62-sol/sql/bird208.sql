SELECT ROUND(AVG(d.A11), 2) AS average_salary,
       COUNT(*) AS district_count
FROM district AS d
WHERE d.A3 = 'east Bohemia'
  AND EXISTS (
      SELECT 1
      FROM account AS a
      JOIN disp AS p ON p.account_id = a.account_id
      JOIN client AS c ON c.client_id = p.client_id
      WHERE a.district_id = d.district_id
        AND c.birth_date >= '1980-01-01'
        AND c.birth_date < '1990-01-01'
  );
