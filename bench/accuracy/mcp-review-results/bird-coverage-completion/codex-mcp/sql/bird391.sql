WITH numeric_tests AS (
    SELECT l.ID, l.Date, l."T-BIL", CAST(l.RNP AS INTEGER) AS rnp_value
    FROM Laboratory AS l
    WHERE l.RNP <> 'negative'
),
strongest_tests AS (
    SELECT ID, Date, "T-BIL"
    FROM numeric_tests
    WHERE rnp_value = (SELECT MAX(rnp_value) FROM numeric_tests)
)
SELECT s."T-BIL", s.Date, s.ID, p.SEX, p.Birthday
FROM strongest_tests AS s
JOIN Patient AS p ON p.ID = s.ID
WHERE s."T-BIL" = (SELECT MAX("T-BIL") FROM strongest_tests)
ORDER BY s.ID, s.Date
LIMIT -1
