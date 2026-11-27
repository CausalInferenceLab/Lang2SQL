WITH college_departments AS (
    SELECT DISTINCT department
    FROM major
    WHERE college = 'College of Humanities and Social Sciences'
), roster AS (
    SELECT EXISTS (
        SELECT 1
        FROM major AS j
        JOIN college_departments AS d ON d.department = j.department
        WHERE j.major_id = m.link_to_major
    ) AS in_college
    FROM member AS m
)
SELECT COUNT(CASE WHEN in_college = 1 THEN 1 END) AS members_in_college,
       COUNT(CASE WHEN in_college = 0 THEN 1 END) AS members_not_in_college
FROM roster;
