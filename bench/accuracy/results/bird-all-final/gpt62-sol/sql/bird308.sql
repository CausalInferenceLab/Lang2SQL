WITH tyler_college AS (
    SELECT mj.college
    FROM member AS m
    JOIN major AS mj ON mj.major_id = m.link_to_major
    WHERE m.first_name = 'Tyler' AND m.last_name = 'Hewitt'
), majors_without_members AS (
    SELECT mj.major_name
    FROM major AS mj
    LEFT JOIN member AS m ON m.link_to_major = mj.major_id
    WHERE mj.college IN (SELECT college FROM tyler_college)
    GROUP BY mj.major_name
    HAVING COUNT(m.link_to_major) = 0
)
SELECT COUNT(*) AS majors_without_enrolled_members
FROM majors_without_members;
