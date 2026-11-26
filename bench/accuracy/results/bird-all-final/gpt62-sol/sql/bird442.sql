WITH care_settings(admission_marker, care_setting) AS (
    VALUES ('+', 'admitted to the hospital'),
           ('-', 'followed at the outpatient clinic')
), eligible_patients AS (
    SELECT DISTINCT p.ID, p.Admission,
           EXISTS (
               SELECT 1
               FROM Laboratory AS l
               WHERE l.ID = p.ID
                 AND CAST(l.RNP AS REAL) >= 4
           ) AS has_elevated_rnp
    FROM Patient AS p
    WHERE p.SEX = 'F'
      AND DATE(p.Birthday) <= DATE('2026-10-09', '-50 years')
      AND p.Admission IN ('+', '-')
)
SELECT s.care_setting,
       COUNT(CASE WHEN p.has_elevated_rnp = 1 THEN 1 END) AS patients_with_anti_rnp_at_least_4,
       COUNT(CASE WHEN p.has_elevated_rnp = 0 THEN 1 END) AS patients_with_no_anti_rnp_at_least_4
FROM care_settings AS s
LEFT JOIN eligible_patients AS p ON p.Admission = s.admission_marker
GROUP BY s.care_setting
ORDER BY s.care_setting;
