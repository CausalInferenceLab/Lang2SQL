SELECT s."School",
       ROUND(f."Percent (%) Eligible FRPM (K-12)", 6) AS "K-12 eligible meal rate"
FROM schools AS s
JOIN frpm AS f ON f."CDSCode" = s."CDSCode"
WHERE s."SOC" = '66'
ORDER BY f."FRPM Count (K-12)" DESC NULLS LAST, s."CDSCode"
LIMIT 5;
