WITH positions(n) AS (
    VALUES (1), (2), (3), (4), (5)
),
short_words AS (
    SELECT DISTINCT words COLLATE BINARY AS word
    FROM main.word_list
    WHERE length(words) BETWEEN 4 AND 5
),
signatures AS (
    SELECT word, (
        SELECT group_concat(letter, '')
        FROM (
            SELECT substr(word, n, 1) AS letter
            FROM positions
            WHERE n <= length(word)
            ORDER BY letter COLLATE BINARY
        )
    ) AS sorted_letters
    FROM short_words
),
counts AS (
    SELECT word, count(*) OVER (PARTITION BY sorted_letters) - 1 AS anagram_count
    FROM signatures
)
SELECT word, anagram_count
FROM counts
WHERE word GLOB 'r*' AND anagram_count > 0
ORDER BY word COLLATE BINARY
LIMIT 10;
