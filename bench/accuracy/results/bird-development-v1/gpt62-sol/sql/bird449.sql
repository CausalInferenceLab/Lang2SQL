WITH urine_results AS (
    SELECT ID, TRIM("U-PRO") AS result
    FROM Laboratory
), urine_concentrations AS (
    SELECT ID,
           CASE
               WHEN SUBSTR(result, 1, 2) = '>=' THEN TRIM(SUBSTR(result, 3))
               WHEN result GLOB '+[0-9](*)' THEN TRIM(SUBSTR(result, 4, LENGTH(result) - 4))
               ELSE result
           END AS mg_dl
    FROM urine_results
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
    FROM urine_concentrations AS u
    WHERE u.ID = p.ID
      AND u.mg_dl GLOB '*[0-9]*'
      AND u.mg_dl NOT GLOB '*[^0-9.]*'
      AND u.mg_dl NOT GLOB '*.*.*'
      AND CAST(u.mg_dl AS REAL) >= 100
)
ORDER BY p.ID;
