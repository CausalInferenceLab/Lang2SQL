WITH eligible_schools AS (
    SELECT
        f."County Name" AS county_name,
        f.CDSCode,
        f."Charter School (Y/N)" AS charter_indicator,
        ROUND(100.0 * f."Free Meal Count (K-12)" / NULLIF(f."Enrollment (K-12)", 0), 2) AS free_meal_pct,
        ROUND(100.0 * f."FRPM Count (K-12)" / NULLIF(f."Enrollment (K-12)", 0), 2) AS frpm_pct
    FROM main.frpm AS f
    JOIN main.schools AS target ON target.County = f."County Name"
    WHERE target.CDSCode = '01612590125716'
      AND f."Free Meal Count (K-12)" > 500
      AND f."FRPM Count (K-12)" < 700
)
SELECT
    e.county_name,
    COUNT(*) AS school_count,
    ROUND(AVG(e.free_meal_pct), 2) AS avg_free_meal_pct,
    ROUND(AVG(e.frpm_pct), 2) AS avg_frpm_pct,
    COUNT(CASE WHEN e.charter_indicator = 1 THEN 1 END) AS charter_school_count,
    COUNT(CASE WHEN e.charter_indicator = 0 THEN 1 END) AS non_charter_school_count,
    ROUND(AVG(s.AvgScrRead + s.AvgScrMath + s.AvgScrWrite), 2) AS avg_sat_score
FROM eligible_schools AS e
LEFT JOIN main.satscores AS s ON s.cds = e.CDSCode
GROUP BY e.county_name
LIMIT 1
