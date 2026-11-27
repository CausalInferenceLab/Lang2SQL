WITH guild_counts AS (
    SELECT LOWER(c.watermark) AS guild,
           COUNT(DISTINCT c.uuid) AS card_count
    FROM cards AS c
    WHERE LOWER(c.watermark) IN (
        'azorius', 'boros', 'dimir', 'golgari', 'gruul',
        'izzet', 'orzhov', 'rakdos', 'selesnya', 'simic'
    )
      AND c.uuid IN (
          SELECT f.uuid
          FROM foreign_data AS f
          WHERE LOWER(f.language) <> 'english'
      )
    GROUP BY LOWER(c.watermark)
)
SELECT UPPER(SUBSTR(guild, 1, 1)) || SUBSTR(guild, 2) AS guild,
       card_count
FROM guild_counts
WHERE card_count = (SELECT MAX(card_count) FROM guild_counts)
ORDER BY guild;
