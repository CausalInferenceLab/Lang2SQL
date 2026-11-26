WITH patient_ages AS (
    SELECT
        CAST(strftime('%Y', '2026-10-09') AS INTEGER)
        - CAST(strftime('%Y', p.Birthday) AS INTEGER)
        - (strftime('%m-%d', p.Birthday) > '10-09') AS age_years
    FROM Patient AS p
    WHERE EXISTS (
        SELECT 1
        FROM Laboratory AS l
        WHERE l.ID = p.ID
          AND CAST(l.RNP AS REAL) >= 64
    )
)
SELECT ROUND(AVG(age_years), 1) AS average_age
FROM patient_ages
WHERE age_years < 70;
