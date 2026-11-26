WITH thrombosis_visits AS (
    SELECT e.ID,
           ',' || REPLACE(REPLACE(e.Diagnosis, '+', ','), ' ', '') || ',' AS diagnoses
    FROM Examination AS e
    WHERE e.Thrombosis > 0
)
SELECT ROUND(
           100.0 * COUNT(DISTINCT CASE WHEN INSTR(diagnoses, ',SLE,') > 0 THEN ID END)
           / NULLIF(COUNT(DISTINCT ID), 0),
           2
       ) AS sle_percentage
FROM thrombosis_visits
LIMIT 1
