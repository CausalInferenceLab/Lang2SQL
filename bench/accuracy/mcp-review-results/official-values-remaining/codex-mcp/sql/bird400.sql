WITH patient_history AS (
    SELECT EXISTS (
        SELECT 1
        FROM main.Laboratory AS l
        WHERE l.ID = p.ID
          AND l.PLT > 100
          AND l.PLT < 400
    ) AS had_normal
    FROM main.Patient AS p
    WHERE p.Diagnosis IN ('RA, SJS', 'RA,SJS', 'SJS, RA', 'SJS  RA')
)
SELECT
    COUNT(*) FILTER (WHERE had_normal = 1) AS ever_normal,
    COUNT(*) FILTER (WHERE had_normal = 0) AS never_normal
FROM patient_history
LIMIT 1000
