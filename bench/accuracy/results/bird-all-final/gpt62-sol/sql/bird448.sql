WITH care_profiles AS (
    SELECT Admission,
           ' ' || UPPER(REPLACE(REPLACE(Diagnosis, ',', ' '), CHAR(29), ' ')) || ' ' AS diagnosis_text
    FROM Patient
    WHERE Admission IN ('+', '-')
),
confirmed_sle AS (
    SELECT Admission
    FROM care_profiles
    WHERE INSTR(REPLACE(diagnosis_text, ' SLE SUSP ', ' '), ' SLE ') > 0
)
SELECT COUNT(*) FILTER (WHERE Admission = '+') AS inpatient_count,
       COUNT(*) FILTER (WHERE Admission = '-') AS outpatient_count,
       ROUND(100.0 * COUNT(*) FILTER (WHERE Admission = '+') / NULLIF(COUNT(*), 0), 2) AS inpatient_percentage
FROM confirmed_sle;
