WITH cohorts(cohort, sort_order, start_date, end_date) AS (
    VALUES ('young', 1, '1980-01-01', NULL),
           ('old',   2, NULL, '1920-01-01')
), cohort_districts AS (
    SELECT DISTINCT c.cohort, a.district_id
    FROM cohorts AS c
    JOIN client AS cl
      ON (c.start_date IS NULL OR cl.birth_date >= c.start_date)
     AND (c.end_date IS NULL OR cl.birth_date < c.end_date)
    JOIN disp AS di ON di.client_id = cl.client_id
    JOIN account AS a ON a.account_id = di.account_id
)
SELECT c.cohort, CAST(ROUND(AVG(d.A11)) AS INTEGER) AS average_salary
FROM cohorts AS c
LEFT JOIN cohort_districts AS cd ON cd.cohort = c.cohort
LEFT JOIN district AS d ON d.district_id = cd.district_id
GROUP BY c.cohort, c.sort_order
ORDER BY c.sort_order;
