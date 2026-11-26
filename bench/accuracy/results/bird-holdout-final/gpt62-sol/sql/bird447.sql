WITH ranked_measurements AS (
    SELECT l.RNP, l.CPK,
           ROW_NUMBER() OVER (PARTITION BY l.ID ORDER BY l.Date DESC) AS rn
    FROM Laboratory AS l
    JOIN Patient AS p ON p.ID = l.ID
    WHERE p.SEX = 'F' AND l.RNP IS NOT NULL
)
SELECT COUNT(*) AS patient_count
FROM ranked_measurements
WHERE rn = 1
  AND CAST(RNP AS INTEGER) > 64
  AND CPK < 250;
