WITH first_examination AS (
    SELECT ID, MIN(JULIANDAY("Examination Date")) AS first_exam_day
    FROM Examination
    GROUP BY ID
)
SELECT ROUND(AVG(e.first_exam_day - JULIANDAY(p."First Date"))) AS average_delay_days
FROM Patient AS p
JOIN first_examination AS e ON e.ID = p.ID
WHERE INSTR(p.Admission, '+') > 0;
