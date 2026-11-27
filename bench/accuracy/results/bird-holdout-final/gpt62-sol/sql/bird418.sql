WITH patient_levels AS (
    SELECT p.ID AS patient_id, MAX(CAST(l.RNP AS INTEGER)) AS anti_rnp_level
    FROM Patient AS p
    JOIN Laboratory AS l ON l.ID = p.ID
    WHERE p."First Date" >= '1991-01-01'
      AND p."First Date" < '1992-01-01'
      AND l.RNP IS NOT NULL
    GROUP BY p.ID
)
SELECT patient_id, anti_rnp_level
FROM patient_levels
WHERE anti_rnp_level = (SELECT MAX(anti_rnp_level) FROM patient_levels)
ORDER BY patient_id;
