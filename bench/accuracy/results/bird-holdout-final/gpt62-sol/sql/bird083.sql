SELECT
    s.type AS expansion_type,
    COUNT(CASE WHEN EXISTS (
        SELECT 1
        FROM set_translations AS st
        WHERE st.setCode = s.code
          AND st.language = 'Japanese'
          AND TRIM(st.translation) <> ''
    ) THEN 1 END) AS japanese_name_count
FROM sets AS s
GROUP BY s.type
ORDER BY s.type;
