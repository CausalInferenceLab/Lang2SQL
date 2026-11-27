SELECT mj.department, COALESCE(SUM(i.amount), 0) AS total_dues
FROM major AS mj
LEFT JOIN member AS m
  ON m.link_to_major = mj.major_id
 AND m.position = 'Member'
 AND EXISTS (
     SELECT 1
     FROM attendance AS a
     JOIN event AS e ON e.event_id = a.link_to_event
     WHERE a.link_to_member = m.member_id
 )
LEFT JOIN income AS i
  ON i.link_to_member = m.member_id
 AND i.source = 'Dues'
WHERE mj.college IN (
    SELECT college
    FROM major
    WHERE department = 'Electrical and Computer Engineering Department'
)
GROUP BY mj.department
ORDER BY mj.department;
