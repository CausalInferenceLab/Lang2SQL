WITH set_status AS (
    SELECT s.type AS set_type,
           s.totalSetSize AS card_count,
           EXISTS (
               SELECT 1
               FROM set_translations AS st
               WHERE st.setCode = s.code
                 AND TRIM(st.translation) <> ''
           ) AS has_translation
    FROM sets AS s
)
SELECT set_type,
       COUNT(CASE WHEN has_translation THEN 1 END) AS translated_set_count,
       CASE WHEN COUNT(CASE WHEN has_translation THEN 1 END) = 0 THEN 0
            ELSE SUM(CASE WHEN has_translation THEN card_count END)
       END AS total_cards
FROM set_status
GROUP BY set_type
ORDER BY set_type;
