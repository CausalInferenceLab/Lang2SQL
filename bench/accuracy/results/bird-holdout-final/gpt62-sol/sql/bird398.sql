SELECT COUNT(DISTINCT p.ID) AS patient_count
FROM Patient AS p
WHERE EXISTS (
    SELECT 1
    FROM Laboratory AS h
    WHERE h.ID = p.ID
      AND (h.HCT <= 29 OR h.HCT >= 52)
)
AND EXISTS (
    SELECT 1
    FROM Laboratory AS r
    WHERE r.ID = p.ID
      AND CAST(r.RNP AS REAL) > 16
);
