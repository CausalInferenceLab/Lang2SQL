WITH fall_events AS (
    SELECT e.event_name, COUNT(a.link_to_member) AS attendees
    FROM main.event AS e
    LEFT JOIN main.attendance AS a ON a.link_to_event = e.event_id
    WHERE e.event_date >= '2019-09-01'
      AND e.event_date < '2019-12-01'
    GROUP BY e.event_id, e.event_name
),
fall_average AS (
    SELECT ROUND(AVG(attendees), 1) AS fall_average_attendance
    FROM fall_events
)
SELECT f.attendees AS womens_soccer_attendance,
       av.fall_average_attendance,
       CASE
           WHEN f.attendees > av.fall_average_attendance THEN 'more'
           WHEN f.attendees < av.fall_average_attendance THEN 'fewer'
           ELSE 'equal'
       END AS verdict
FROM fall_events AS f
CROSS JOIN fall_average AS av
WHERE f.event_name = 'Women''s Soccer'
LIMIT 1000
