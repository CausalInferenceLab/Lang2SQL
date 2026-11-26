SELECT ROUND(AVG(d.A11), 2) AS average_district_salary
FROM main.district AS d
WHERE d.district_id IN (
    SELECT a.district_id
    FROM main.loan AS l
    JOIN main.account AS a ON a.account_id = l.account_id
    WHERE l.amount < 100000
      AND l.status = 'C'
)
LIMIT 1000
