WITH starting_grades AS (
    SELECT TRIM(SUBSTR(GSserved, 1, INSTR(GSserved || '-', '-') - 1)) AS grade_level
    FROM schools
    WHERE NCESDist = '0632640'
), ranked_grades AS (
    SELECT grade_level,
           CASE
               WHEN grade_level = 'P' THEN -2
               WHEN grade_level = 'TK' THEN -1
               WHEN grade_level = 'K' THEN 0
               WHEN grade_level <> '' AND grade_level NOT GLOB '*[^0-9]*'
                   THEN CAST(grade_level AS INTEGER)
           END AS grade_order
    FROM starting_grades
)
SELECT grade_level
FROM ranked_grades
WHERE grade_order IS NOT NULL
ORDER BY grade_order
LIMIT 1;
