SELECT COUNT(DISTINCT i.session) AS unique_sessions
FROM form_log AS i
WHERE i.path = '/regist/input'
  AND i.action = 'view'
  AND EXISTS (
    SELECT 1
    FROM form_log AS c
    WHERE c.session = i.session
      AND c.path = '/regist/confirm'
      AND c.action = 'view'
      AND c.stamp > i.stamp
  );
