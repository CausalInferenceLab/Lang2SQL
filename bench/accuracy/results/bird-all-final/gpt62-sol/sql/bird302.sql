WITH fall_event_counts AS (
    SELECT e.event_id,
           e.event_name,
           COUNT(DISTINCT a.link_to_member) AS student_count
    FROM event AS e
    LEFT JOIN attendance AS a ON a.link_to_event = e.event_id
    WHERE e.event_date >= '2019-09-01'
      AND e.event_date < '2019-12-01'
    GROUP BY e.event_id, e.event_name
), comparison AS (
    SELECT MAX(CASE WHEN event_name = 'Women''s Soccer' THEN student_count END) AS womens_soccer_attendance,
           ROUND(AVG(student_count), 1) AS fall_2019_average_attendance
    FROM fall_event_counts
)
SELECT womens_soccer_attendance,
       fall_2019_average_attendance,
       CASE
           WHEN womens_soccer_attendance > fall_2019_average_attendance THEN 'more'
           WHEN womens_soccer_attendance < fall_2019_average_attendance THEN 'fewer'
           WHEN womens_soccer_attendance = fall_2019_average_attendance THEN 'equal'
       END AS verdict
FROM comparison;
