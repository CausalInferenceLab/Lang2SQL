WITH RECURSIVE numbers AS (
  SELECT 1 AS n UNION ALL SELECT n + 1 FROM numbers WHERE n < 5
),
filtered_words AS (
  SELECT words
  FROM word_list
  WHERE length(words) BETWEEN 4 AND 5
    AND words LIKE 'r%'
),
anagram_groups AS (
  SELECT group_key, COUNT(*) AS anagram_count
  FROM (
    SELECT words,
      (SELECT group_concat(substr(words, n, 1), '')
       FROM numbers
       WHERE n <= length(words)
       ORDER BY substr(words, n, 1)) AS group_key
    FROM filtered_words
  )
  GROUP BY group_key
  HAVING COUNT(*) > 1
),
anagram_words AS (
  SELECT fw.words AS word, ag.anagram_count
  FROM filtered_words fw
  JOIN anagram_groups ag ON (
    SELECT group_concat(substr(fw.words, n, 1), '')
    FROM numbers
    WHERE n <= length(fw.words)
    ORDER BY substr(fw.words, n, 1)
  ) = ag.group_key
)
SELECT word, anagram_count
FROM anagram_words
ORDER BY word
LIMIT 10;
