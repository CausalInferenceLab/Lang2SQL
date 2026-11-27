WITH target_county AS (
    SELECT DISTINCT County
    FROM schools
    WHERE CDSCode = '01612590125716'
)
SELECT
    c.County AS county_name,
    COUNT(f.CDSCode) AS school_count,
    ROUND(AVG(ROUND(100.0 * f."Free Meal Count (K-12)" / f."Enrollment (K-12)", 2)), 2) AS avg_free_meal_percentage,
    ROUND(AVG(ROUND(100.0 * f."FRPM Count (K-12)" / f."Enrollment (K-12)", 2)), 2) AS avg_frpm_percentage,
    COUNT(CASE WHEN f."Charter School (Y/N)" = 1 THEN 1 END) AS charter_school_count,
    COUNT(CASE WHEN f."Charter School (Y/N)" = 0 THEN 1 END) AS non_charter_school_count,
    ROUND(AVG(s.AvgScrRead + s.AvgScrMath + s.AvgScrWrite), 2) AS avg_sat_score
FROM target_county AS c
LEFT JOIN frpm AS f
    ON f."County Name" = c.County
    AND f."Free Meal Count (K-12)" > 500
    AND f."FRPM Count (K-12)" < 700
LEFT JOIN satscores AS s ON s.cds = f.CDSCode
GROUP BY c.County;
