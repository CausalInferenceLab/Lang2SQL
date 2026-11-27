SELECT ROUND(AVG(d.A11), 2) AS average_salary
FROM district AS d
WHERE d.A3 = 'central Bohemia'
  AND EXISTS (
    SELECT 1
    FROM client AS c
    JOIN disp AS owner ON owner.client_id = c.client_id
    WHERE c.district_id = d.district_id
      AND owner.type = 'OWNER'
      AND NOT EXISTS (
        SELECT 1
        FROM disp AS account_disp
        JOIN card AS cc ON cc.disp_id = account_disp.disp_id
        WHERE account_disp.account_id = owner.account_id
      )
  );
