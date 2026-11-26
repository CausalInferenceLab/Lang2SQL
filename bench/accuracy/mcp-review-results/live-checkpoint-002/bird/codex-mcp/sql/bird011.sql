WITH eligible AS (
    SELECT s.School, s.GSserved,
           substr(s.GSserved, 1, instr(s.GSserved || '-', '-') - 1) AS low_grade
    FROM main.schools AS s
    WHERE s.NCESDist = '0613360'
      AND s.StatusType = 'Active'
      AND s.School IS NOT NULL
      AND (s.GSserved LIKE '%-12' OR s.GSserved = '12')
      AND NOT EXISTS (
          SELECT 1
          FROM main.satscores AS t
          WHERE t.cds = s.CDSCode
            AND t.NumTstTakr > 0
      )
),
graded AS (
    SELECT School, GSserved,
           CASE
               WHEN low_grade IN ('P', 'PS', 'PK') THEN -1
               WHEN low_grade = 'K' THEN 0
               ELSE CAST(low_grade AS INTEGER)
           END AS grade_order
    FROM eligible
)
SELECT School, GSserved
FROM graded
WHERE grade_order = (SELECT MIN(grade_order) FROM graded)
ORDER BY School
LIMIT -1
