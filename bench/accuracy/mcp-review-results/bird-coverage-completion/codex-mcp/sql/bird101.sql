SELECT s.name, s.releaseDate
FROM sets AS s
WHERE s.type IN (
    SELECT ref.type
    FROM sets AS ref
    WHERE ref.name = 'From the Vault: Lore'
)
AND NOT EXISTS (
    SELECT 1
    FROM set_translations AS t
    WHERE t.setCode = s.code
      AND t.language <> 'English'
      AND t.translation IS NOT NULL
      AND t.translation <> ''
)
ORDER BY s.releaseDate ASC
LIMIT -1
