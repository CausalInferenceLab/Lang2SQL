WITH ranked_tests AS (
    SELECT l.ID, l.Date, l.DNA,
           RANK() OVER (ORDER BY CAST(l.DNA AS REAL) DESC) AS level_rank
    FROM Laboratory AS l
    JOIN Patient AS p ON p.ID = l.ID
    WHERE p.SEX = 'F'
      AND p.Admission = '+'
      AND l.DNA IS NOT NULL
)
SELECT ID, Date, DNA
FROM ranked_tests
WHERE level_rank = 1
ORDER BY ID
LIMIT -1
