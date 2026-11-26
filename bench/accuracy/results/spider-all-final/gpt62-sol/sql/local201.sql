WITH dictionary AS (
    SELECT DISTINCT words COLLATE BINARY AS word
    FROM word_list
    WHERE length(words) BETWEEN 4 AND 5
), positions(pos) AS (
    VALUES (1), (2), (3), (4), (5)
), signatures AS (
    SELECT word, pos,
           group_concat(substr(word, pos, 1), '') OVER (
               PARTITION BY word COLLATE BINARY
               ORDER BY substr(word, pos, 1) COLLATE BINARY
               ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
           ) AS signature
    FROM dictionary
    JOIN positions ON pos <= length(word)
), counts AS (
    SELECT word,
           count(*) OVER (PARTITION BY signature COLLATE BINARY) - 1 AS anagram_count
    FROM signatures
    WHERE pos = 1
)
SELECT word, anagram_count
FROM counts
WHERE substr(word, 1, 1) COLLATE BINARY = 'r'
  AND anagram_count > 0
ORDER BY word COLLATE BINARY
LIMIT 10;
