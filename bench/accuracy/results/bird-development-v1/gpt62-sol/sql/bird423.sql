SELECT COUNT(DISTINCT a.ID) AS patient_count
FROM Laboratory AS a
WHERE LOWER(TRIM(a.SSA)) NOT IN ('negative', '0', '')
  AND EXISTS (
    SELECT 1
    FROM Laboratory AS u
    WHERE u.ID = a.ID
      AND LOWER(TRIM(u."U-PRO")) NOT IN ('negative', '-', 'none', '0', '')
  );
