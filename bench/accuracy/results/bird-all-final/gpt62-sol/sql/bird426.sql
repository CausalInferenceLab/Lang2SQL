SELECT MAX(CAST(NULLIF(RNP, 'negative') AS NUMERIC)) AS highest_anti_rnp
FROM Laboratory
WHERE CRE < 1.5;
