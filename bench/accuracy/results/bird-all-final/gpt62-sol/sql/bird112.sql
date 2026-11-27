SELECT COUNT(*) AS printing_count
FROM cards AS c
WHERE c.originalReleaseDate >= '2014-01-01'
  AND c.originalReleaseDate < '2015-01-01'
  AND EXISTS (
    SELECT 1
    FROM sets AS s
    WHERE s.code = c.setCode
      AND s.mtgoCode IS NOT NULL
      AND TRIM(s.mtgoCode) <> ''
  );
