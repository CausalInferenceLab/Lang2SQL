SELECT DISTINCT name
FROM cards
WHERE isPromo = 1
  AND otherFaceIds IS NOT NULL
  AND SUBSTR(originalReleaseDate, 1, 4) = '2019'
ORDER BY name;
