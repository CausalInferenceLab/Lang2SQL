SELECT f."Free Meal Count (K-12)" / NULLIF(f."Enrollment (K-12)", 0) * 100.0
           AS "Percent (%) Eligible Free (K-12)",
       f."District Code"
FROM "frpm" AS f
JOIN "schools" AS s ON s."CDSCode" = f."CDSCode"
WHERE 'Alusine' IN (s."AdmFName1", s."AdmFName2", s."AdmFName3")
LIMIT 1000
