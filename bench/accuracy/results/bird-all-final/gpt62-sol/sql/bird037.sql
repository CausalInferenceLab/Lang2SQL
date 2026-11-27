WITH math_benchmark AS (
    SELECT MAX(a.AvgScrMath) AS highest_avg_sat_math
    FROM satscores AS a
    JOIN schools AS s ON s.CDSCode = a.cds
    WHERE s.StatusType = 'Active'
      AND a.rtype = 'S'
      AND a.NumTstTakr >= 10
)
SELECT s.School AS school_name,
       s.Phone AS phone_number,
       s.Website AS website,
       a.AvgScrRead AS avg_sat_reading,
       a.AvgScrMath AS avg_sat_math,
       a.AvgScrWrite AS avg_sat_writing,
       (s.Charter = 1) AS is_charter,
       f."Enrollment (K-12)" AS k12_enrollment_2014_2015,
       ROUND(100.0 * f."Percent (%) Eligible FRPM (K-12)", 2) AS pct_eligible_frpm_k12,
       b.highest_avg_sat_math,
       b.highest_avg_sat_math - a.AvgScrMath AS sat_math_points_below_highest
FROM schools AS s
LEFT JOIN satscores AS a ON a.cds = s.CDSCode AND a.rtype = 'S'
LEFT JOIN frpm AS f ON f.CDSCode = s.CDSCode AND f."Academic Year" = '2014-2015'
CROSS JOIN math_benchmark AS b
WHERE s.CDSCode = '07618040113118';
