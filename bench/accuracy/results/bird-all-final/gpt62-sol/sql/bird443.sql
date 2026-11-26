WITH ranked_igg AS (
    SELECT ID, IGG,
           ROW_NUMBER() OVER (PARTITION BY ID ORDER BY "Date" DESC) AS rn
    FROM Laboratory
    WHERE IGG IS NOT NULL
)
SELECT COUNT(*) AS patient_count
FROM ranked_igg AS l
WHERE l.rn = 1
  AND l.IGG >= 2000
  AND EXISTS (
      SELECT 1
      FROM Patient AS p
      WHERE p.ID = l.ID AND p.Admission = '-'
  );
