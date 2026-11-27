WITH power_counts AS (
    SELECT hero_id, COUNT(DISTINCT power_id) AS power_count
    FROM hero_power
    GROUP BY hero_id
)
SELECT g.gender,
       ROUND(AVG(COALESCE(p.power_count, 0)), 2) AS average_superpowers
FROM superhero AS h
JOIN gender AS g ON g.id = h.gender_id
LEFT JOIN power_counts AS p ON p.hero_id = h.id
WHERE g.gender IN ('Male', 'Female')
  AND INSTR(TRIM(h.full_name), ' ') > 0
GROUP BY g.gender;
