WITH RECURSIVE diagnosis_tokens(patient_id, diagnosis, remaining) AS (
    SELECT Patient.ID,
           '',
           CASE trim(Patient.Diagnosis, char(9, 10, 11, 12, 13, 32))
               WHEN 'SJS  RA' THEN 'SJS,RA'
               WHEN 'RA  SJS' THEN 'RA,SJS'
               ELSE replace(Patient.Diagnosis, char(29), ',')
           END || ','
    FROM Patient
    WHERE instr(Patient.Diagnosis, 'RA') > 0
      AND instr(Patient.Diagnosis, 'SJS') > 0

    UNION ALL

    SELECT patient_id,
           trim(substr(remaining, 1, instr(remaining, ',') - 1),
                char(9, 10, 11, 12, 13, 32)),
           substr(remaining, instr(remaining, ',') + 1)
    FROM diagnosis_tokens
    WHERE remaining <> ''
),
eligible_patients AS (
    SELECT patient_id
    FROM diagnosis_tokens
    WHERE diagnosis <> ''
      AND rtrim(diagnosis, '.') NOT LIKE '% susp'
    GROUP BY patient_id
    HAVING COUNT(DISTINCT diagnosis) = 2
       AND MIN(diagnosis) = 'RA'
       AND MAX(diagnosis) = 'SJS'
),
normal_platelet_patients AS (
    SELECT DISTINCT Laboratory.ID
    FROM Laboratory
    WHERE Laboratory.PLT > 100
      AND Laboratory.PLT < 400
)
SELECT COUNT(normal_platelet_patients.ID) AS patients_with_normal_platelets,
       COUNT(*) - COUNT(normal_platelet_patients.ID) AS patients_without_normal_platelets
FROM eligible_patients
LEFT JOIN normal_platelet_patients
    ON normal_platelet_patients.ID = eligible_patients.patient_id
LIMIT 1
