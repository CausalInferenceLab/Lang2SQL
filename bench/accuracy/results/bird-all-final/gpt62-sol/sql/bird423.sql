SELECT COUNT(DISTINCT p.ID) AS patient_count
FROM Patient AS p
WHERE EXISTS (
    SELECT 1
    FROM Laboratory AS l
    WHERE l.ID = p.ID
      AND LOWER(TRIM(l.SSA)) NOT IN ('negative', '0')
)
AND EXISTS (
    SELECT 1
    FROM Laboratory AS l
    WHERE l.ID = p.ID
      AND LOWER(TRIM(l."U-PRO")) NOT IN ('negative', 'none', '-', '0')
);
