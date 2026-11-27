WITH age_categories(age_category, sort_order) AS (
    VALUES ('20s', 1), ('30s', 2), ('40s', 3), ('50s', 4), ('others', 5)
), ages AS (
    SELECT user_id,
           CAST(strftime('%Y', 'now') AS INTEGER)
           - CAST(strftime('%Y', birth_date) AS INTEGER)
           - (strftime('%m-%d', 'now') < strftime('%m-%d', birth_date)) AS age
    FROM mst_users
), categorized_users AS (
    SELECT user_id,
           CASE
               WHEN age BETWEEN 20 AND 29 THEN '20s'
               WHEN age BETWEEN 30 AND 39 THEN '30s'
               WHEN age BETWEEN 40 AND 49 THEN '40s'
               WHEN age BETWEEN 50 AND 59 THEN '50s'
               ELSE 'others'
           END AS age_category
    FROM ages
)
SELECT c.age_category, COUNT(DISTINCT u.user_id) AS user_count
FROM age_categories AS c
LEFT JOIN categorized_users AS u ON u.age_category = c.age_category
GROUP BY c.age_category, c.sort_order
ORDER BY c.sort_order;
