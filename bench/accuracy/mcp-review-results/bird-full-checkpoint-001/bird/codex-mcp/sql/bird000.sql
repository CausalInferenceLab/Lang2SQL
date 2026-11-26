SELECT MAX(
    f."Free Meal Count (Ages 5-17)" / NULLIF(f."Enrollment (Ages 5-17)", 0)
) AS highest_free_meal_eligibility_rate
FROM frpm AS f
JOIN satscores AS s ON s.cds = f.CDSCode
WHERE s.rtype = 'S'
  AND CAST(s.NumGE1500 AS REAL) / NULLIF(s.NumTstTakr, 0) > 0.3
LIMIT 1
