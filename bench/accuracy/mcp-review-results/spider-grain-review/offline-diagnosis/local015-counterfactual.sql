WITH helmet_codes(equipment, helmet_used) AS (
    VALUES
        ('driver, motorcycle helmet used', 1),
        ('passenger, motorcycle helmet used', 1),
        ('driver, motorcycle helmet not used', 0),
        ('passenger, motorcycle helmet not used', 0)
),
collision_groups AS (
    SELECT DISTINCT p.case_id, h.helmet_used
    FROM main.parties AS p
    JOIN helmet_codes AS h
      ON h.equipment IN (p.party_safety_equipment_1, p.party_safety_equipment_2)
)
SELECT CASE WHEN g.helmet_used = 1 THEN 'helmet_used' ELSE 'helmet_not_used' END AS helmet_usage, 100.0 * SUM(c.motorcyclist_killed_count) / COUNT(*) AS fatality_percentage
FROM main.collisions AS c JOIN collision_groups AS g ON g.case_id = c.case_id
WHERE c.motorcycle_collision = 1 GROUP BY g.helmet_used ORDER BY g.helmet_used DESC