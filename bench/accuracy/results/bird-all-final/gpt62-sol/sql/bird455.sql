SELECT COUNT(DISTINCT ID) AS patient_count
FROM Patient
WHERE Diagnosis COLLATE NOCASE = 'psoriatic arthritis';
