SELECT frameVersion, COUNT(*) AS card_count
FROM cards
WHERE frameVersion = (
    SELECT MAX(frameVersion)
    FROM cards
    WHERE convertedManaCost = 0
)
GROUP BY frameVersion;
