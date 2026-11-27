SELECT SUM(s.totalSetSize) AS total_cards
FROM sets AS s
WHERE s.releaseDate >= '2017-01-01'
  AND s.releaseDate < '2018-01-01'
  AND EXISTS (
    SELECT 1
    FROM set_translations AS t
    WHERE t.setCode = s.code
      AND TRIM(t.language) <> ''
      AND TRIM(t.language) COLLATE NOCASE <> 'English'
      AND TRIM(t.translation) <> ''
  );
