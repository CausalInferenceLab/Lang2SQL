WITH non_stack_max AS (
    SELECT name, version, step, MAX(test_score) AS max_test_score
    FROM model_score
    WHERE model <> 'Stack' AND step IN (1, 2, 3)
    GROUP BY name, version, step
), stack_wins AS (
    SELECT n.name, COUNT(*) AS win_count
    FROM non_stack_max AS n
    WHERE EXISTS (
        SELECT 1
        FROM model_score AS s
        WHERE s.name = n.name
          AND s.version = n.version
          AND s.step = n.step
          AND s.model = 'Stack'
          AND s.test_score > n.max_test_score
    )
    GROUP BY n.name
)
SELECT p.name
FROM problem AS p
JOIN stack_wins AS w ON w.name = p.name
WHERE w.win_count > (
    SELECT COUNT(*)
    FROM solution AS sol
    WHERE sol.name = p.name
)
ORDER BY p.name;
