WITH platelet_history AS (
    SELECT EXISTS (
        SELECT 1
        FROM main.Laboratory AS l
        WHERE l.ID = p.ID
          AND l.PLT > 100
          AND l.PLT < 400
    ) AS had_normal_platelets
    FROM main.Patient AS p
    WHERE p.Diagnosis IN ('RA, SJS', 'RA,SJS', 'SJS, RA', 'SJS  RA')
)
SELECT COUNT(*) FILTER (WHERE had_normal_platelets = 1) AS ever_normal_platelets,
       COUNT(*) FILTER (WHERE had_normal_platelets = 0) AS never_normal_platelets
FROM platelet_history
LIMIT 1
