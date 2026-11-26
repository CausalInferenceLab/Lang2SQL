WITH tylers_college AS (
    SELECT m.college
    FROM main.member AS t
    JOIN main.major AS m ON m.major_id = t.link_to_major
    WHERE t.first_name = 'Tyler' AND t.last_name = 'Hewitt'
),
unrepresented_majors AS (
    SELECT m.major_name
    FROM main.major AS m
    LEFT JOIN main.member AS c ON c.link_to_major = m.major_id
    WHERE m.college IN (SELECT college FROM tylers_college)
    GROUP BY m.major_name
    HAVING COUNT(c.link_to_major) = 0
)
SELECT COUNT(major_name) AS majors_without_members
FROM unrepresented_majors
LIMIT 1
