SELECT MAX(CAST(RNP AS NUMERIC)) AS highest_anti_rnp
FROM main.Laboratory
WHERE CRE < 1.5
  AND RNP <> 'negative'
LIMIT 1
