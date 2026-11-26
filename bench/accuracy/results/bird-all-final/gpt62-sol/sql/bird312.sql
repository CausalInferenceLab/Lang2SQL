SELECT ROUND(100.0 * COUNT(CASE WHEN NOT EXISTS (
    SELECT 1
    FROM member AS m
    WHERE m.link_to_major = a.major_id
) THEN 1 END) / COUNT(*), 2) AS percentage
FROM major AS a;
