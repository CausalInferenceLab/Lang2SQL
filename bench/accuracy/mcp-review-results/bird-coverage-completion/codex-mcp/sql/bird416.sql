WITH high_creatinine AS (
    SELECT DISTINCT l.ID
    FROM main.Laboratory AS l
    WHERE l.CRE >= 1.5
)
SELECT
    p.SEX,
    COUNT(h.ID) AS patients_with_high_creatinine,
    COUNT(*) AS total_patients,
    ROUND(100.0 * COUNT(h.ID) / COUNT(*), 1) AS percentage
FROM main.Patient AS p
LEFT JOIN high_creatinine AS h ON h.ID = p.ID
WHERE p.SEX IN ('M', 'F')
GROUP BY p.SEX
ORDER BY p.SEX DESC
LIMIT 2
