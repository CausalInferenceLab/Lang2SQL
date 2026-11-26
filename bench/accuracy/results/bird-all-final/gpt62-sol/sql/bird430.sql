WITH patient_groups(admission, patient_group, sort_order) AS (
    VALUES ('+', 'admitted to the hospital', 1),
           ('-', 'followed at the outpatient clinic', 2)
), patient_admissions AS (
    SELECT DISTINCT ID, Admission
    FROM Patient
    WHERE Admission IN ('+', '-')
)
SELECT g.patient_group,
       COUNT(l.LDH) AS normal_ldh_measurements
FROM patient_groups AS g
LEFT JOIN patient_admissions AS p ON p.Admission = g.admission
LEFT JOIN Laboratory AS l ON l.ID = p.ID AND l.LDH < 500
GROUP BY g.sort_order, g.patient_group
ORDER BY g.sort_order;
