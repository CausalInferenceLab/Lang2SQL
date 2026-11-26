WITH cohort_districts AS (
    SELECT DISTINCT
        CASE WHEN c.birth_date >= '1980-01-01' THEN 'young' ELSE 'old' END AS cohort,
        a.district_id
    FROM main.client AS c
    JOIN main.disp AS p ON p.client_id = c.client_id
    JOIN main.account AS a ON a.account_id = p.account_id
    WHERE c.birth_date >= '1980-01-01' OR c.birth_date < '1920-01-01'
)
SELECT cd.cohort, CAST(ROUND(AVG(d.A11)) AS INTEGER) AS average_salary
FROM cohort_districts AS cd
JOIN main.district AS d ON d.district_id = cd.district_id
GROUP BY cd.cohort
ORDER BY CASE WHEN cd.cohort = 'young' THEN 1 ELSE 2 END
LIMIT 2
