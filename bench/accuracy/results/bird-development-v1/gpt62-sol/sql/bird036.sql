WITH eligible AS (
    SELECT s.CDSCode,
           s.School AS school_name,
           s.Charter AS charter,
           s.GSserved AS grades_served,
           CAST(f."Free Meal Count (K-12)" AS REAL) / f."Enrollment (K-12)" AS free_meal_rate
    FROM schools AS s
    JOIN frpm AS f ON f.CDSCode = s.CDSCode
    WHERE s.County = 'Alameda'
      AND (TRIM(s.GSserved) = '12' OR TRIM(s.GSserved) LIKE '%-12')
      AND f."Enrollment (K-12)" > 0
      AND f."Free Meal Count (K-12)" IS NOT NULL
)
SELECT e.school_name,
       PRINTF('%.4f', e.free_meal_rate) AS free_meal_rate,
       CASE e.charter WHEN 1 THEN 'Yes' WHEN 0 THEN 'No' ELSE 'Unknown' END AS charter_school,
       e.grades_served,
       CASE
           WHEN a.AvgScrRead IS NULL OR a.AvgScrMath IS NULL OR a.AvgScrWrite IS NULL THEN 'No SAT Data'
           WHEN a.AvgScrRead + a.AvgScrMath + a.AvgScrWrite < 1200 THEN 'Below Average'
           WHEN a.AvgScrRead + a.AvgScrMath + a.AvgScrWrite <= 1500 THEN 'Average'
           ELSE 'Above Average'
       END AS sat_performance_level
FROM eligible AS e
LEFT JOIN satscores AS a ON a.cds = e.CDSCode
WHERE e.free_meal_rate = (SELECT MAX(free_meal_rate) FROM eligible)
ORDER BY e.school_name, e.CDSCode;
