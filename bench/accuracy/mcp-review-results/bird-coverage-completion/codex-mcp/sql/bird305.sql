SELECT
    maj.department,
    COALESCE(SUM(i.amount), 0) AS total_dues
FROM major AS maj
LEFT JOIN member AS m
    ON m.link_to_major = maj.major_id
    AND m.position = 'Member'
    AND m.member_id IN (SELECT a.link_to_member FROM attendance AS a)
LEFT JOIN income AS i
    ON i.link_to_member = m.member_id
    AND i.source = 'Dues'
    AND NULLIF(TRIM(i.date_received), '') IS NOT NULL
WHERE maj.college IN (
    SELECT college
    FROM major
    WHERE department = 'Electrical and Computer Engineering Department'
)
GROUP BY maj.department
ORDER BY maj.department
LIMIT -1
