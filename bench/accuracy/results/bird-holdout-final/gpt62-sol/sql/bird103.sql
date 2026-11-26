SELECT s.code, s.releaseDate
FROM sets AS s
WHERE s.releaseDate >= '2020-01-01'
  AND s.releaseDate < '2021-01-01'
  AND NOT EXISTS (
    SELECT 1
    FROM set_translations AS st
    WHERE st.setCode = s.code
      AND st.language = 'Spanish'
      AND NULLIF(TRIM(st.translation), '') IS NOT NULL
  )
ORDER BY s.releaseDate, s.code;
