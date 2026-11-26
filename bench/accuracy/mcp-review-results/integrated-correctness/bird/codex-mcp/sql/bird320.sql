WITH college_counts AS (
    SELECT a.college, COUNT(DISTINCT a.major_id) AS major_count
    FROM main.member AS m
    JOIN main.major AS a ON a.major_id = m.link_to_major
    GROUP BY a.college
)
SELECT college, major_count
FROM college_counts
WHERE major_count = (SELECT MAX(major_count) FROM college_counts)
ORDER BY college
LIMIT 1000
