WITH block_sets AS (
    SELECT s.block, s.baseSetSize,
           EXISTS (
               SELECT 1
               FROM set_translations AS st
               WHERE st.setCode = s.code
                 AND st.translation <> ''
           ) AS has_translation
    FROM sets AS s
    WHERE s.block COLLATE BINARY IN ('Masques', 'Mirage')
)
SELECT block,
       COUNT(*) FILTER (WHERE has_translation) AS translated_set_count,
       SUM(baseSetSize) FILTER (WHERE has_translation) AS total_base_set_cards
FROM block_sets
GROUP BY block
ORDER BY block;
