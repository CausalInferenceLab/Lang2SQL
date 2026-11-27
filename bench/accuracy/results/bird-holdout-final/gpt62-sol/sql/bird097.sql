SELECT DISTINCT name
FROM cards
WHERE isPromo = 1
  AND otherFaceIds IS NOT NULL
  AND CAST(frameVersion AS INTEGER) = (
    SELECT MAX(CAST(frameVersion AS INTEGER))
    FROM cards
  )
ORDER BY name;
