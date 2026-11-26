SELECT s.name, s.releaseDate
FROM sets AS s
WHERE s.type IN (
    SELECT ref.type
    FROM sets AS ref
    WHERE ref.name = 'From the Vault: Lore'
)
AND NOT EXISTS (
    SELECT 1
    FROM set_translations AS st
    WHERE st.setCode = s.code
      AND TRIM(st.translation) <> ''
)
ORDER BY s.releaseDate ASC, s.name ASC;
