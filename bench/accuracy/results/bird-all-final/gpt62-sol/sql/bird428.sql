WITH eligible_tests AS (
    SELECT ID, Date, RNP, CAST(NULLIF(RNP, 'negative') AS INTEGER) AS rnp_value
    FROM Laboratory
    WHERE GOT >= 60
)
SELECT ID, Date, RNP
FROM eligible_tests
WHERE rnp_value = (SELECT MAX(rnp_value) FROM eligible_tests);
