SELECT c.colors AS card_color,
       COUNT(DISTINCT l.format) AS legal_format_count
FROM cards AS c
LEFT JOIN legalities AS l
       ON l.uuid = c.uuid AND l.status = 'Legal'
WHERE c.id BETWEEN 1 AND 20
GROUP BY c.colors
ORDER BY c.colors;
