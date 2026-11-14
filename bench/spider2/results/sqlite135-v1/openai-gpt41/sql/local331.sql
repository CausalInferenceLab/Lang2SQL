WITH ordered_logs AS (
  SELECT 
    session, 
    user_id, 
    action, 
    stamp, 
    ROW_NUMBER() OVER (PARTITION BY session ORDER BY stamp) AS rn
  FROM access_log
),
page_triples AS (
  SELECT 
    l1.session, 
    l1.user_id, 
    l1.stamp AS stamp1, 
    l2.stamp AS stamp2, 
    l3.stamp AS stamp3,
    l1.action AS action1, 
    l2.action AS action2, 
    l3.action AS action3
  FROM ordered_logs l1
  JOIN ordered_logs l2 ON l1.session = l2.session AND l1.rn + 1 = l2.rn
  JOIN ordered_logs l3 ON l1.session = l3.session AND l1.rn + 2 = l3.rn
)
SELECT 
  action3 AS third_page_visit, 
  COUNT(*) AS occurrences
FROM page_triples
WHERE action1 = '/detail' AND action2 = '/detail' AND action3 != '/detail'
GROUP BY action3
ORDER BY occurrences DESC
LIMIT 3;
