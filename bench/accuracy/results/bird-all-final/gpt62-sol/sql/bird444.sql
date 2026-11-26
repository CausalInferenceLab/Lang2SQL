WITH sle_results AS (
    SELECT p.ID, p.Diagnosis, l.DNA,
           CAST(l.DNA AS REAL) AS dna_value
    FROM Patient AS p
    JOIN Laboratory AS l ON l.ID = p.ID
    WHERE p.Diagnosis LIKE '%SLE%'
      AND p.Diagnosis NOT LIKE '%SLE susp%'
      AND l.DNA IS NOT NULL
)
SELECT ID, Diagnosis, MIN(DNA) AS highest_anti_dna
FROM sle_results
WHERE dna_value = (SELECT MAX(dna_value) FROM sle_results)
GROUP BY ID, Diagnosis
ORDER BY ID;
