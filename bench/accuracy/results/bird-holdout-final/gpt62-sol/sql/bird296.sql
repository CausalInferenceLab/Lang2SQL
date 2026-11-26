SELECT SUM(b.spent) FILTER (WHERE e.event_date < '2020-01-01')
     - SUM(b.spent) FILTER (WHERE e.event_date >= '2020-01-01') AS spending_difference
FROM event AS e
JOIN budget AS b ON b.link_to_event = e.event_id
WHERE e.event_date >= '2019-01-01'
  AND e.event_date < '2021-01-01';
