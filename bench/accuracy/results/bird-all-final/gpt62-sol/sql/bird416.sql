WITH patients_by_sex AS (
    SELECT DISTINCT ID, SEX
    FROM Patient
    WHERE SEX IN ('M', 'F')
), abnormal_creatinine AS (
    SELECT DISTINCT ID
    FROM Laboratory
    WHERE CRE >= 1.5
)
SELECT
    p.SEX AS sex,
    COUNT(a.ID) AS patients_with_abnormal_creatinine,
    COUNT(*) AS total_patients,
    ROUND(100.0 * COUNT(a.ID) / COUNT(*), 1) AS percentage
FROM patients_by_sex AS p
LEFT JOIN abnormal_creatinine AS a ON a.ID = p.ID
GROUP BY p.SEX
ORDER BY p.SEX DESC;
