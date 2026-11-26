WITH eligible_schools AS (
    SELECT s.CDSCode,
           s.School AS school_name,
           s.Charter AS charter,
           s.GSserved AS grades_served,
           1.0 * f."Free Meal Count (K-12)" / f."Enrollment (K-12)" AS free_meal_rate
    FROM schools AS s
    JOIN frpm AS f ON f.CDSCode = s.CDSCode
    WHERE s.County = 'Alameda'
      AND (TRIM(s.GSserved) = '12' OR TRIM(s.GSserved) LIKE '%-12')
), top_schools AS (
    SELECT CDSCode, school_name, charter, grades_served, free_meal_rate
    FROM eligible_schools
    WHERE free_meal_rate = (SELECT MAX(free_meal_rate) FROM eligible_schools)
), sat_totals AS (
    SELECT cds, AvgScrRead + AvgScrMath + AvgScrWrite AS sat_total
    FROM satscores
    WHERE rtype = 'S'
)
SELECT t.school_name,
       ROUND(t.free_meal_rate, 4) AS free_meal_rate,
       CASE t.charter WHEN 1 THEN 'Yes' WHEN 0 THEN 'No' ELSE 'Unknown' END AS charter_school,
       t.grades_served,
       CASE
           WHEN a.sat_total IS NULL THEN 'No SAT Data'
           WHEN a.sat_total < 1200 THEN 'Below Average'
           WHEN a.sat_total <= 1500 THEN 'Average'
           ELSE 'Above Average'
       END AS sat_performance_level
FROM top_schools AS t
LEFT JOIN sat_totals AS a ON a.cds = t.CDSCode
ORDER BY t.school_name, t.CDSCode;
