SELECT ROUND(
    100.0 * COUNT(DISTINCT CASE
        WHEN ',' || REPLACE(REPLACE(UPPER("Diagnosis"), ' ', ''), '+', ',') || ',' LIKE '%,SLE,%'
        THEN "ID"
    END) / NULLIF(COUNT(DISTINCT "ID"), 0),
    2
) AS sle_percentage
FROM "Examination"
WHERE "Thrombosis" > 0;
