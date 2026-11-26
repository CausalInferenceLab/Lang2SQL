SELECT COUNT(*) AS italian_set_count
FROM sets AS s
WHERE s.block = 'Ice Age'
  AND s.code IN (
    SELECT st.setCode
    FROM set_translations AS st
    WHERE st.language = 'Italian'
  )
LIMIT 1
