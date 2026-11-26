WITH sexes(code, sex) AS (
    VALUES ('F', 'female'), ('M', 'male')
)
SELECT s.sex, COUNT(DISTINCT p.ID) AS patient_count
FROM sexes AS s
LEFT JOIN Patient AS p
    ON UPPER(TRIM(p.SEX)) IN (s.code, UPPER(s.sex))
    AND EXISTS (
        SELECT 1
        FROM Laboratory AS l
        WHERE l.ID = p.ID AND l.HGB < 7.0
    )
GROUP BY s.sex
ORDER BY s.sex;
