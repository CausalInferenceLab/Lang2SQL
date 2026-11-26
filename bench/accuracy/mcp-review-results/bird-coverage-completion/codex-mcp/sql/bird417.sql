WITH ranked_patients AS (
    SELECT
        p.Admission,
        p.ID,
        MAX(l."T-CHO") AS highest_reading,
        RANK() OVER (
            PARTITION BY p.Admission
            ORDER BY MAX(l."T-CHO") DESC
        ) AS reading_rank
    FROM main.Patient AS p
    JOIN main.Laboratory AS l ON l.ID = p.ID
    WHERE p.Admission IN ('+', '-')
      AND l."T-CHO" IS NOT NULL
    GROUP BY p.Admission, p.ID
)
SELECT highest_reading AS "T-CHO", Admission, ID
FROM ranked_patients
WHERE reading_rank = 1
ORDER BY CASE WHEN Admission = '+' THEN 0 ELSE 1 END, ID
LIMIT -1
