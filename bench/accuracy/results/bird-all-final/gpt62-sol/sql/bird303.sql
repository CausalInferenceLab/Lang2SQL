WITH college_departments AS (
    SELECT DISTINCT department
    FROM major
    WHERE college = 'College of Humanities and Social Sciences'
), member_counts AS (
    SELECT link_to_major, COUNT(*) AS member_count
    FROM member
    GROUP BY link_to_major
), department_majors AS (
    SELECT m.department, m.major_name, COALESCE(mc.member_count, 0) AS member_count
    FROM major AS m
    JOIN college_departments AS d ON d.department = m.department
    LEFT JOIN member_counts AS mc ON mc.link_to_major = m.major_id
    ORDER BY m.department, m.major_name, m.major_id
)
SELECT department,
       GROUP_CONCAT(major_name, ', ') AS majors_offered,
       GROUP_CONCAT(major_name, ', ') FILTER (WHERE member_count > 0) AS majors_held_by_members,
       ROUND(AVG(member_count), 2) AS avg_members_per_major
FROM department_majors
GROUP BY department
ORDER BY department;
