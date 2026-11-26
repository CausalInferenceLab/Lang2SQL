WITH ages AS (
    SELECT user_id,
           strftime('%Y', 'now') - strftime('%Y', birth_date)
           - (strftime('%m-%d', 'now') < strftime('%m-%d', birth_date)) AS age
    FROM main.mst_users
),
classified AS (
    SELECT user_id,
           CASE
               WHEN age BETWEEN 20 AND 29 THEN '20s'
               WHEN age BETWEEN 30 AND 39 THEN '30s'
               WHEN age BETWEEN 40 AND 49 THEN '40s'
               WHEN age BETWEEN 50 AND 59 THEN '50s'
               ELSE 'others'
           END AS age_category
    FROM ages
),
categories(age_category) AS (
    VALUES ('20s'), ('30s'), ('40s'), ('50s'), ('others')
)
SELECT c.age_category, COUNT(DISTINCT u.user_id) AS user_count
FROM categories AS c
LEFT JOIN classified AS u ON u.age_category = c.age_category
GROUP BY c.age_category
ORDER BY c.age_category
LIMIT 5
