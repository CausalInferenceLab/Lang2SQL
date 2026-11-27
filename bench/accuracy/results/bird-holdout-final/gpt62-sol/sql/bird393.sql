WITH patient_titers AS (
    SELECT l.ID AS patient_id,
           MAX(CAST(l.RNP AS INTEGER)) AS anti_rnp_titer
    FROM Laboratory AS l
    WHERE LOWER(TRIM(l.RNP)) <> 'negative'
      AND EXISTS (
          SELECT 1
          FROM Patient AS p
          WHERE p.ID = l.ID AND p.Admission = '-'
      )
      AND EXISTS (
          SELECT 1
          FROM Laboratory AS s
          WHERE s.ID = l.ID
            AND LOWER(TRIM(s.SSA)) NOT IN ('negative', '0')
      )
    GROUP BY l.ID
)
SELECT patient_id, anti_rnp_titer
FROM patient_titers
WHERE anti_rnp_titer = (SELECT MAX(anti_rnp_titer) FROM patient_titers)
ORDER BY patient_id;
