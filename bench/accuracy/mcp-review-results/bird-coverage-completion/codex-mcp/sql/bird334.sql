SELECT
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE a.alignment = 'Neutral')
        / NULLIF(COUNT(*), 0),
        2
    ) AS neutral_percentage,
    COUNT(*) FILTER (
        WHERE a.alignment = 'Neutral' AND p.publisher_name = 'Marvel Comics'
    ) AS neutral_marvel_count
FROM main.superhero AS s
LEFT JOIN main.alignment AS a ON a.id = s.alignment_id
LEFT JOIN main.publisher AS p ON p.id = s.publisher_id
LIMIT 1000
