SELECT
    s.name,
    s.keyruneCode,
    (
        SELECT COUNT(DISTINCT st.language)
        FROM set_translations AS st
        WHERE st.setCode = s.code
          AND NULLIF(TRIM(st.translation), '') IS NOT NULL
    ) AS language_count
FROM sets AS s
WHERE s.releaseDate >= '2020-01-01'
  AND s.releaseDate < '2021-01-01'
ORDER BY s.releaseDate, s.name, s.code;
