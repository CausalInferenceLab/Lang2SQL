SELECT
    COUNT(*) FILTER (WHERE p.SEX = 'F') AS women,
    COUNT(*) FILTER (WHERE p.SEX = 'M') AS men
FROM main.Patient AS p
WHERE p.ID IN (
    SELECT l.ID
    FROM main.Laboratory AS l
    WHERE CAST(l.RNP AS REAL) > 32
)
LIMIT 1
