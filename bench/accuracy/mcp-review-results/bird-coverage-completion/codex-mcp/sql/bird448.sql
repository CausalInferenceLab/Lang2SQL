WITH care_counts AS (
    SELECT
        COUNT(*) FILTER (WHERE p.Admission = '+') AS inpatients,
        COUNT(*) FILTER (WHERE p.Admission = '-') AS outpatients
    FROM main.Patient AS p
    WHERE p.Diagnosis LIKE '%SLE%'
      AND p.Diagnosis NOT LIKE '%SLE susp%'
)
SELECT
    inpatients,
    outpatients,
    ROUND(100.0 * inpatients / NULLIF(inpatients + outpatients, 0), 2) AS inpatient_percentage
FROM care_counts
LIMIT 1
