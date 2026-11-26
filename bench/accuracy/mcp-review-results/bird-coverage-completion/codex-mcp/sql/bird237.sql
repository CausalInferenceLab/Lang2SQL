SELECT ROUND(AVG(d.A11), 2) AS average_salary_czk
FROM district AS d
WHERE d.district_id IN (
    SELECT c.district_id
    FROM client AS c
    WHERE c.gender = 'F'
      AND c.birth_date < '1950-01-01'
)
LIMIT 1000
