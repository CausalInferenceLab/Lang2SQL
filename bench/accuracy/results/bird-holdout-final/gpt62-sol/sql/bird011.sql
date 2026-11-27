WITH eligible AS (
    SELECT s.School AS school_name,
           TRIM(s.GSserved) AS grade_span_served
    FROM schools AS s
    WHERE s.NCESDist = '0613360'
      AND s.StatusType = 'Active'
      AND s.School IS NOT NULL
      AND (TRIM(s.GSserved) = '12' OR TRIM(s.GSserved) LIKE '%-12')
      AND NOT EXISTS (
          SELECT 1
          FROM satscores AS sat
          WHERE sat.cds = s.CDSCode
            AND sat.NumTstTakr > 0
      )
), starting_grades AS (
    SELECT school_name,
           grade_span_served,
           CASE
               WHEN INSTR(grade_span_served, '-') = 0 THEN grade_span_served
               ELSE TRIM(SUBSTR(grade_span_served, 1, INSTR(grade_span_served, '-') - 1))
           END AS start_grade
    FROM eligible
), ranked_grades AS (
    SELECT school_name,
           grade_span_served,
           CASE
               WHEN UPPER(start_grade) IN ('P', 'PK', 'PS', 'PRE', 'PRESCHOOL') THEN -2
               WHEN UPPER(start_grade) = 'TK' THEN -1
               WHEN UPPER(start_grade) = 'K' THEN 0
               ELSE CAST(start_grade AS INTEGER)
           END AS grade_order
    FROM starting_grades
)
SELECT school_name, grade_span_served
FROM ranked_grades
WHERE grade_order = (SELECT MIN(grade_order) FROM ranked_grades)
ORDER BY school_name COLLATE NOCASE;
