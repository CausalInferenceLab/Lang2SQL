WITH helmet_by_collision AS (
    SELECT
        case_id,
        MAX(CASE WHEN party_safety_equipment_1 IN (
            'driver, motorcycle helmet used', 'passenger, motorcycle helmet used'
        ) OR party_safety_equipment_2 IN (
            'driver, motorcycle helmet used', 'passenger, motorcycle helmet used'
        ) THEN 1 ELSE 0 END) AS helmet_used,
        MAX(CASE WHEN party_safety_equipment_1 IN (
            'driver, motorcycle helmet not used', 'passenger, motorcycle helmet not used'
        ) OR party_safety_equipment_2 IN (
            'driver, motorcycle helmet not used', 'passenger, motorcycle helmet not used'
        ) THEN 1 ELSE 0 END) AS helmet_not_used
    FROM parties
    GROUP BY case_id
)
SELECT
    100.0 * SUM(CASE WHEN h.helmet_used = 1 THEN c.motorcyclist_killed_count END)
        / NULLIF(COUNT(CASE WHEN h.helmet_used = 1 THEN 1 END), 0) AS helmet_used_fatality_pct,
    100.0 * SUM(CASE WHEN h.helmet_not_used = 1 THEN c.motorcyclist_killed_count END)
        / NULLIF(COUNT(CASE WHEN h.helmet_not_used = 1 THEN 1 END), 0) AS helmet_not_used_fatality_pct
FROM collisions AS c
JOIN helmet_by_collision AS h ON h.case_id = c.case_id
WHERE c.motorcycle_collision = 1;
