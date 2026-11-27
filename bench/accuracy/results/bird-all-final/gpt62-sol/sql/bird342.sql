WITH publisher_counts AS (
    SELECT COUNT(CASE WHEN p.publisher_name = 'DC Comics' THEN 1 END) AS dc_count,
           COUNT(CASE WHEN p.publisher_name = 'Marvel Comics' THEN 1 END) AS marvel_count
    FROM superhero AS s
    JOIN publisher AS p ON p.id = s.publisher_id
    WHERE p.publisher_name IN ('DC Comics', 'Marvel Comics')
)
SELECT CASE
           WHEN dc_count > marvel_count THEN 'DC Comics'
           WHEN marvel_count > dc_count THEN 'Marvel Comics'
           ELSE 'Tie'
       END AS publisher,
       ABS(dc_count - marvel_count) AS count_difference
FROM publisher_counts;
