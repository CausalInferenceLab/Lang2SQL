SELECT MAX(CAST(NULLIF(l.RNP, 'negative') AS NUMERIC)) AS highest_anti_rnp
FROM Laboratory AS l
JOIN Patient AS p ON p.ID = l.ID
WHERE p.Admission = '-';
