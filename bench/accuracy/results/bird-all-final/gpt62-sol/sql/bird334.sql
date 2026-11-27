SELECT
    ROUND(
        100.0 * COUNT(CASE WHEN a.alignment = 'Neutral' COLLATE NOCASE THEN 1 END)
        / NULLIF(COUNT(*), 0),
        2
    ) AS neutral_percentage,
    COUNT(CASE
        WHEN a.alignment = 'Neutral' COLLATE NOCASE
         AND p.publisher_name = 'Marvel Comics' COLLATE NOCASE
        THEN 1
    END) AS neutral_marvel_count
FROM superhero AS s
LEFT JOIN alignment AS a ON a.id = s.alignment_id
LEFT JOIN publisher AS p ON p.id = s.publisher_id;
