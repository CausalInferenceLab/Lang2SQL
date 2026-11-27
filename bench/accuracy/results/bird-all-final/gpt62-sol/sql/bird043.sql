SELECT f."County Name" AS county_name,
       COUNT(DISTINCT f.CDSCode) AS qualifying_schools
FROM frpm AS f
WHERE f."Academic Year" = '2014-2015'
  AND CAST(f."Free Meal Count (K-12)" AS REAL) / NULLIF(f."Enrollment (K-12)", 0) > 0.1
  AND EXISTS (
      SELECT 1
      FROM satscores AS s
      WHERE s.cds = f.CDSCode
        AND s.rtype = 'S'
        AND CAST(s.NumGE1500 AS INTEGER) >= 1
  )
GROUP BY f."County Name"
ORDER BY qualifying_schools DESC, county_name ASC;
