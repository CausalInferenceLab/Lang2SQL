WITH sex_groups(sex) AS (
    VALUES ('F'), ('M')
)
SELECT s.sex, COUNT(DISTINCT p.ID) AS patient_count
FROM sex_groups AS s
LEFT JOIN Patient AS p
    ON p.SEX = s.sex
   AND EXISTS (
       SELECT 1
       FROM Laboratory AS l
       WHERE l.ID = p.ID AND l.ALB < 3.5
   )
GROUP BY s.sex
ORDER BY s.sex;
