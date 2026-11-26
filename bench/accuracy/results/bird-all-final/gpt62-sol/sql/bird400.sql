WITH RECURSIVE diagnosis_parts (ID, diagnosis, remaining) AS (
    SELECT ID, NULL,
           CASE
               WHEN TRIM(UPPER(Diagnosis)) IN ('RA  SJS', 'SJS  RA') THEN 'RA,SJS'
               ELSE REPLACE(UPPER(Diagnosis), CHAR(29), ',')
           END || ','
    FROM Patient
    WHERE Diagnosis IS NOT NULL

    UNION ALL

    SELECT ID,
           TRIM(SUBSTR(remaining, 1, INSTR(remaining, ',') - 1), ' ' || CHAR(9, 10, 11, 13)),
           SUBSTR(remaining, INSTR(remaining, ',') + 1)
    FROM diagnosis_parts
    WHERE remaining <> ''
), overlap_patients AS (
    SELECT ID
    FROM diagnosis_parts
    WHERE diagnosis <> ''
      AND diagnosis NOT LIKE '% SUSP'
      AND diagnosis NOT LIKE '% SUSP.'
    GROUP BY ID
    HAVING COUNT(DISTINCT diagnosis) = 2
       AND MAX(diagnosis = 'RA') = 1
       AND MAX(diagnosis = 'SJS') = 1
), platelet_history AS (
    SELECT p.ID, EXISTS (
        SELECT 1
        FROM Laboratory AS l
        WHERE l.ID = p.ID
          AND l.PLT > 100
          AND l.PLT < 400
    ) AS has_normal_platelet
    FROM overlap_patients AS p
)
SELECT COUNT(CASE WHEN has_normal_platelet = 1 THEN 1 END) AS ever_normal_platelet,
       COUNT(CASE WHEN has_normal_platelet = 0 THEN 1 END) AS never_normal_platelet
FROM platelet_history;
