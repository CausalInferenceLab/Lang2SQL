SELECT MAX(f."Percent (%) Eligible Free (Ages 5-17)") AS highest_free_meal_eligibility_rate
FROM frpm AS f
JOIN satscores AS s ON s.cds = f.CDSCode
WHERE s.rtype = 'S'
  AND 1.0 * s.NumGE1500 / NULLIF(s.NumTstTakr, 0) > 0.3;
