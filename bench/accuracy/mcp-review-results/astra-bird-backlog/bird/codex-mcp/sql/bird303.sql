WITH members_per_major AS (
    SELECT m.department, m.major_id, m.major_name, COUNT(r.member_id) AS member_count
    FROM main.major AS m
    LEFT JOIN main.member AS r ON r.link_to_major = m.major_id
    WHERE m.department IN (
        SELECT department
        FROM main.major
        WHERE college = 'College of Humanities and Social Sciences'
    )
    GROUP BY m.department, m.major_id, m.major_name
)
SELECT department,
       json_group_array(major_name ORDER BY major_name, major_id) AS majors_offered,
       json_group_array(major_name ORDER BY major_name, major_id)
           FILTER (WHERE member_count > 0) AS majors_held_by_members,
       ROUND(AVG(member_count), 2) AS average_members_per_major
FROM members_per_major
GROUP BY department
ORDER BY department
LIMIT 1000
