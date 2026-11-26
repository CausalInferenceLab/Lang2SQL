WITH protein_results AS (
    SELECT ID, TRIM("U-PRO") AS result
    FROM Laboratory
), protein_amounts AS (
    SELECT ID,
           TRIM(CASE
               WHEN result GLOB '>=*' THEN SUBSTR(result, 3)
               WHEN result GLOB '+[0-9](*)' THEN SUBSTR(result, 4, LENGTH(result) - 4)
               ELSE result
           END) AS amount
    FROM protein_results
)
SELECT p.ID, p.SEX, p.Birthday
FROM Patient AS p
WHERE EXISTS (
    SELECT 1
    FROM Laboratory AS l
    WHERE l.ID = p.ID AND l.LDH >= 500
)
AND EXISTS (
    SELECT 1
    FROM protein_amounts AS u
    WHERE u.ID = p.ID
      AND u.amount GLOB '[0-9]*'
      AND u.amount NOT GLOB '*[^0-9.]*'
      AND u.amount NOT GLOB '*.*.*'
      AND CAST(u.amount AS REAL) >= 100
)
ORDER BY p.ID;
