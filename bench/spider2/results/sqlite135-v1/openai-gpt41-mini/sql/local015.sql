WITH CollisionHelmetInfo AS (
  SELECT
    c.case_id,
    MAX(CASE WHEN p.party_safety_equipment_1 LIKE '%helmet%' OR p.party_safety_equipment_2 LIKE '%helmet%' THEN 1 ELSE 0 END) AS has_helmet
  FROM collisions c
  LEFT JOIN parties p ON c.case_id = p.case_id
  WHERE p.statewide_vehicle_type = 'motorcycle'
  GROUP BY c.case_id
), FatalitiesByHelmet AS (
  SELECT
    chi.has_helmet AS has_helmet,
    COUNT(DISTINCT v.case_id) AS fatalities
  FROM CollisionHelmetInfo chi
  LEFT JOIN victims v ON chi.case_id = v.case_id AND v.victim_role = 'motorcyclist' AND v.victim_degree_of_injury = 'fatal'
  GROUP BY chi.has_helmet
), TotalCollisionsByHelmet AS (
  SELECT
    has_helmet AS has_helmet,
    COUNT(DISTINCT case_id) AS total_collisions
  FROM CollisionHelmetInfo
  GROUP BY has_helmet
)
SELECT
  CASE f.has_helmet
    WHEN 1 THEN 'helmet_worn'
    WHEN 0 THEN 'no_helmet'
  END AS helmet_usage_group,
  100.0 * CAST(f.fatalities AS FLOAT) / t.total_collisions AS fatality_rate
FROM FatalitiesByHelmet f
JOIN TotalCollisionsByHelmet t ON f.has_helmet = t.has_helmet
ORDER BY helmet_usage_group;
