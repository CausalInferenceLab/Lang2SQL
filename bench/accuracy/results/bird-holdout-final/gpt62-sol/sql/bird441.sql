WITH RECURSIVE diagnosis_parts(ID, SEX, diagnosis, remaining) AS (
    SELECT p.ID, p.SEX, '',
           REPLACE(REPLACE(UPPER(p.Diagnosis), CHAR(29), ','), '  ', ',') || ','
    FROM Patient AS p
    WHERE EXISTS (
        SELECT 1
        FROM Laboratory AS l
        WHERE l.ID = p.ID AND l."T-BIL" >= 1.2
    )
    UNION ALL
    SELECT ID, SEX,
           TRIM(SUBSTR(remaining, 1, INSTR(remaining, ',') - 1)),
           SUBSTR(remaining, INSTR(remaining, ',') + 1)
    FROM diagnosis_parts
    WHERE remaining <> ''
)
SELECT DISTINCT ID, SEX
FROM diagnosis_parts
WHERE diagnosis IN ('SLE', 'SLE (DIFFUSE LE)')
ORDER BY SEX, ID;
