SELECT ROUND(100.0 * AVG(EXISTS (
    SELECT 1
    FROM foreign_data AS f
    WHERE f.uuid = c.uuid
      AND f.language = 'French'
)), 2) AS french_translation_percentage
FROM cards AS c
WHERE c.isStorySpotlight = 1;
