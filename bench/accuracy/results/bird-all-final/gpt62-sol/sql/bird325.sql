WITH members_by_type AS (
    SELECT e.type AS event_type,
           COUNT(DISTINCT a.link_to_member) AS member_count
    FROM event AS e
    LEFT JOIN attendance AS a ON a.link_to_event = e.event_id
    WHERE e.event_date >= '2019-09-01'
      AND e.event_date < '2020-06-01'
    GROUP BY e.type
)
SELECT event_type, member_count
FROM members_by_type
WHERE member_count = (SELECT MAX(member_count) FROM members_by_type)
ORDER BY event_type;
