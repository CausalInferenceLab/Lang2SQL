WITH highest_rnp_tests AS (
    SELECT ID, Date, "T-BIL"
    FROM Laboratory
    WHERE CAST(NULLIF(RNP, 'negative') AS REAL) = (
        SELECT MAX(CAST(NULLIF(RNP, 'negative') AS REAL))
        FROM Laboratory
    )
)
SELECT l."T-BIL" AS total_bilirubin,
       l.Date AS test_date,
       l.ID AS patient_id,
       p.SEX AS sex,
       p.Birthday AS birthday
FROM highest_rnp_tests AS l
LEFT JOIN Patient AS p ON p.ID = l.ID
WHERE l."T-BIL" = (SELECT MAX("T-BIL") FROM highest_rnp_tests);
