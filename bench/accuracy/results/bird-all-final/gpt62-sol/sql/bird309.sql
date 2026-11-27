WITH event_attendance AS (
  SELECT e.event_id, COUNT(DISTINCT a.link_to_member) AS member_count
  FROM event AS e
  LEFT JOIN attendance AS a ON a.link_to_event = e.event_id
  WHERE EXISTS (
    SELECT 1
    FROM budget AS b
    WHERE b.link_to_event = e.event_id
      AND b.category = 'Advertisement'
  )
  GROUP BY e.event_id
)
SELECT ROUND(AVG(member_count), 2) AS average_members_attended
FROM event_attendance;
