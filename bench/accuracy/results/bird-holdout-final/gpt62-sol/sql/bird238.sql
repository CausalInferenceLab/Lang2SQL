SELECT ROUND(AVG(d.A11), 2) AS average_district_salary
FROM district AS d
WHERE EXISTS (
    SELECT 1
    FROM account AS a
    JOIN loan AS l ON l.account_id = a.account_id
    WHERE a.district_id = d.district_id
      AND l.amount < 100000
      AND l.status = 'C'
);
