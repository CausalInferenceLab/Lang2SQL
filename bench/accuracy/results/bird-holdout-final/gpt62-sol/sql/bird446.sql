WITH latest_visit AS (
    SELECT ID, MAX("Date") AS visit_date
    FROM Laboratory
    GROUP BY ID
)
SELECT COUNT(DISTINCT CASE WHEN p.SEX = 'F' THEN p.ID END)
     - COUNT(DISTINCT CASE WHEN p.SEX = 'M' THEN p.ID END) AS female_minus_male
FROM latest_visit AS v
JOIN Laboratory AS l ON l.ID = v.ID AND l."Date" = v.visit_date
JOIN Patient AS p ON p.ID = v.ID
WHERE CAST(l.RNP AS REAL) >= 32;
