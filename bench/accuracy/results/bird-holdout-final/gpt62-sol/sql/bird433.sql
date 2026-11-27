WITH patient_peaks AS (
    SELECT p.ID, p.SEX, MAX(CAST(l.RNP AS INTEGER)) AS max_rnp
    FROM Patient AS p
    JOIN Laboratory AS l ON l.ID = p.ID
    WHERE p.Admission = '-'
      AND l.RNP IS NOT NULL
      AND LOWER(TRIM(l.RNP)) <> 'negative'
      AND EXISTS (
          SELECT 1
          FROM Laboratory AS a
          WHERE a.ID = p.ID AND a.HGB < 10
      )
    GROUP BY p.ID, p.SEX
)
SELECT ID, SEX, max_rnp
FROM patient_peaks
WHERE max_rnp = (SELECT MAX(max_rnp) FROM patient_peaks)
ORDER BY ID;
