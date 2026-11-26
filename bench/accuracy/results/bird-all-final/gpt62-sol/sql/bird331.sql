SELECT COUNT(DISTINCT j.major_id) AS major_count
FROM member AS m
JOIN major AS j ON j.major_id = m.link_to_major
WHERE j.department = 'Political Science Department';
