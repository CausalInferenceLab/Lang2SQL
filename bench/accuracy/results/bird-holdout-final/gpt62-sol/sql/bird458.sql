WITH sexes(sex) AS (
    VALUES ('F'), ('M')
)
SELECT s.sex, COUNT(DISTINCT p.ID) AS patient_count
FROM sexes AS s
LEFT JOIN Patient AS p
    ON p.SEX = s.sex
   AND EXISTS (SELECT 1 FROM Laboratory AS l WHERE l.ID = p.ID)
GROUP BY s.sex
ORDER BY s.sex;
