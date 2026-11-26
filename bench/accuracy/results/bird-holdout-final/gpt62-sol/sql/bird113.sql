WITH printing_counts AS (
  SELECT f.language, COUNT(*) AS printing_count
  FROM foreign_data AS f
  WHERE f.language <> 'English'
    AND EXISTS (
      SELECT 1
      FROM set_translations AS st
      WHERE st.language = f.language
    )
  GROUP BY f.language
)
SELECT language, printing_count
FROM printing_counts
WHERE printing_count = (SELECT MIN(printing_count) FROM printing_counts)
ORDER BY language;
