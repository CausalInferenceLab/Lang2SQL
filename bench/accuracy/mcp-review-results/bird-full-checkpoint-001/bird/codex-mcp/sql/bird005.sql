SELECT COUNT(*) AS school_count
FROM main.frpm
WHERE "County Name" = 'Los Angeles'
  AND "Charter School (Y/N)" = 0
  AND 100.0 * "Free Meal Count (K-12)" / NULLIF("Enrollment (K-12)", 0) < 0.18
LIMIT 1
