WITH college_counts AS (
    SELECT ma.college, COUNT(*) AS number_of_majors
    FROM member AS m
    JOIN major AS ma ON ma.major_id = m.link_to_major
    GROUP BY ma.college
)
SELECT college, number_of_majors
FROM college_counts
WHERE number_of_majors = (SELECT MAX(number_of_majors) FROM college_counts)
ORDER BY college;
