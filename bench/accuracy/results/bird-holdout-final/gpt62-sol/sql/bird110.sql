WITH eligible_creatures AS (
    SELECT DISTINCT c.name, c.convertedManaCost AS mana_value
    FROM cards AS c
    WHERE c.edhrecRank BETWEEN 1 AND 1000
      AND (',' || REPLACE(c.types, ' ', '') || ',') LIKE '%,Creature,%'
      AND EXISTS (
          SELECT 1
          FROM legalities AS l
          WHERE l.uuid = c.uuid
            AND l.status = 'Banned' COLLATE NOCASE
      )
)
SELECT ROUND(AVG(mana_value), 1) AS average_mana_value
FROM eligible_creatures;
