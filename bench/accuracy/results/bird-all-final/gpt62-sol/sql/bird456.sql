WITH sexes(code, sex) AS (
    VALUES ('M', 'Male'), ('F', 'Female')
)
SELECT s.sex,
       (
           SELECT COUNT(DISTINCT p.ID)
           FROM Patient AS p
           WHERE p.SEX = s.code
             AND NOT EXISTS (
                 SELECT 1
                 FROM Laboratory AS l
                 WHERE l.ID = p.ID AND l.GPT >= 60
             )
       ) AS patient_count
FROM sexes AS s
ORDER BY s.code DESC;
