SELECT m.first_name, m.last_name, m.phone
FROM member AS m
WHERE EXISTS (
    SELECT 1
    FROM expense AS e
    WHERE e.link_to_member = m.member_id
      AND e.cost > (SELECT AVG(cost) FROM expense)
);
