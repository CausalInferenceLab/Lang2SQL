SELECT ROUND(
    100.0 * COUNT(DISTINCT CASE WHEN Birthday >= '1960-01-01' THEN ID END)
    / NULLIF(COUNT(DISTINCT ID), 0),
    1
) AS percentage
FROM Patient
WHERE LOWER(TRIM(Diagnosis)) = 'psoriatic arthritis';
