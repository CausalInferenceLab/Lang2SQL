WITH filtered_words AS (
    SELECT word, LENGTH(word) AS len
    FROM word_list
    WHERE LENGTH(word) IN (4, 5)
      AND word LIKE 'r%'
),
word_signatures AS (
    SELECT 
        word, 
        len,
        -- Create a canonical signature for the word: sorted characters (case sensitive)
        (SELECT GROUP_CONCAT(c, '') FROM (
            SELECT SUBSTR(word, pos, 1) AS c
            FROM (
                SELECT word, len, GENERATE_SERIES(1, len) AS pos
            )
            WHERE word_list.word = word
            ORDER BY c
        )) AS signature
    FROM filtered_words
),
anagram_counts AS (
    SELECT 
        w1.word,
        w1.len,
        w1.signature,
        COUNT(w2.word) AS anagram_count
    FROM word_signatures w1
    JOIN word_signatures w2
      ON w1.len = w2.len
     AND w1.signature = w2.signature
     -- Don't group identical words
     AND w1.word <> w2.word
    GROUP BY w1.word, w1.len, w1.signature
)
SELECT
    word,
    len,
    anagram_count
FROM anagram_counts
WHERE anagram_count > 0
ORDER BY word ASC
LIMIT 10;
