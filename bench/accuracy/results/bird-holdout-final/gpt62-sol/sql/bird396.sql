WITH eligible_tests AS (
    SELECT l.ID, l."Date", l.DNA, CAST(l.DNA AS REAL) AS dna_level
    FROM Laboratory AS l
    WHERE l.DNA IS NOT NULL
      AND EXISTS (
          SELECT 1
          FROM Patient AS p
          WHERE p.ID = l.ID
            AND p.SEX = 'F'
            AND p.Admission = '+'
      )
)
SELECT ID, "Date", DNA
FROM eligible_tests
WHERE dna_level = (SELECT MAX(dna_level) FROM eligible_tests)
ORDER BY ID;
