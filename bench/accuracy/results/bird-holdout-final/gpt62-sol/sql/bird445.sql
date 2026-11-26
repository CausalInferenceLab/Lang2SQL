WITH ranked_igg AS (
    SELECT ID, IGG,
           ROW_NUMBER() OVER (PARTITION BY ID ORDER BY "Date" DESC) AS rn
    FROM Laboratory
    WHERE IGG IS NOT NULL
)
SELECT CASE p.Admission
           WHEN '+' THEN 'admitted to the hospital'
           WHEN '-' THEN 'followed at the outpatient clinic'
       END AS care_setting,
       ROUND(100.0 * COUNT(CASE WHEN i.IGG > 900 AND i.IGG < 2000 THEN 1 END)
             / COUNT(*), 1) AS percentage
FROM Patient AS p
LEFT JOIN ranked_igg AS i ON i.ID = p.ID AND i.rn = 1
WHERE p.Admission IN ('+', '-')
GROUP BY p.Admission
ORDER BY p.Admission;
