WITH race_entries AS (
    SELECT ra.year, ra.round, re.driver_id, re.constructor_id
    FROM results AS re
    JOIN races AS ra ON ra.race_id = re.race_id
    WHERE ra.year BETWEEN 1950 AND 1959
), season_bounds AS (
    SELECT year, driver_id, MIN(round) AS first_round, MAX(round) AS last_round
    FROM race_entries
    GROUP BY year, driver_id
    HAVING COUNT(DISTINCT round) >= 2
)
SELECT d.full_name
FROM drivers AS d
WHERE EXISTS (
    SELECT 1
    FROM season_bounds AS sb
    JOIN race_entries AS first_entry
      ON first_entry.year = sb.year
     AND first_entry.driver_id = sb.driver_id
     AND first_entry.round = sb.first_round
    JOIN race_entries AS last_entry
      ON last_entry.year = sb.year
     AND last_entry.driver_id = sb.driver_id
     AND last_entry.round = sb.last_round
     AND last_entry.constructor_id = first_entry.constructor_id
    WHERE sb.driver_id = d.driver_id
)
ORDER BY d.full_name;
