WITH ranked_patients AS (
    SELECT MAX(l."T-CHO") AS "T-CHO",
           p.Admission,
           p.ID,
           RANK() OVER (
               PARTITION BY p.Admission
               ORDER BY MAX(l."T-CHO") DESC
           ) AS cholesterol_rank
    FROM Patient AS p
    JOIN Laboratory AS l ON l.ID = p.ID
    WHERE p.Admission IN ('+', '-')
      AND l."T-CHO" IS NOT NULL
    GROUP BY p.Admission, p.ID
)
SELECT "T-CHO", Admission, ID
FROM ranked_patients
WHERE cholesterol_rank = 1
ORDER BY CASE Admission WHEN '+' THEN 0 ELSE 1 END, ID;
