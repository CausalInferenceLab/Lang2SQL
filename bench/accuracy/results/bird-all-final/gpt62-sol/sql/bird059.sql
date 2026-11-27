SELECT COUNT(*) AS set_count
FROM sets AS s
WHERE s.block = 'Ice Age'
  AND EXISTS (
    SELECT 1
    FROM set_translations AS st
    WHERE st.setCode = s.code
      AND st.language = 'Italian'
  );
