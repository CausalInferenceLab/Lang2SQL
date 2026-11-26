WITH sexes(sex) AS (
    VALUES ('F'), ('M')
)
SELECT s.sex, COUNT(DISTINCT l.ID) AS patient_count
FROM sexes AS s
LEFT JOIN Patient AS p ON p.SEX = s.sex
LEFT JOIN Laboratory AS l ON l.ID = p.ID
    AND l."Date" >= '1994-01-01'
    AND l."Date" < '1995-01-01'
    AND l.GOT < 60
GROUP BY s.sex
ORDER BY s.sex;
