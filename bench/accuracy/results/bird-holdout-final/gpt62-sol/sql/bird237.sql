SELECT ROUND(AVG(d.A11), 2) AS average_district_salary
FROM district AS d
WHERE EXISTS (
    SELECT 1
    FROM client AS c
    WHERE c.district_id = d.district_id
      AND c.gender = 'F'
      AND c.birth_date < '1950-01-01'
);
