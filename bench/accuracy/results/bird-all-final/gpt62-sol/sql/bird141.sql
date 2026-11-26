WITH matching_posts AS (
    SELECT Id, Title
    FROM posts
    WHERE LOWER(Title) LIKE '%principal component scores%'
), linked_posts AS (
    SELECT p.Id AS post_id, l.RelatedPostId AS linked_post_id
    FROM matching_posts AS p
    JOIN postLinks AS l ON l.PostId = p.Id
    UNION ALL
    SELECT p.Id AS post_id, l.PostId AS linked_post_id
    FROM matching_posts AS p
    JOIN postLinks AS l ON l.RelatedPostId = p.Id
), related_counts AS (
    SELECT p.Title, COUNT(DISTINCT l.linked_post_id) AS related_post_count
    FROM matching_posts AS p
    LEFT JOIN linked_posts AS l
        ON l.post_id = p.Id AND l.linked_post_id <> p.Id
    GROUP BY p.Id, p.Title
)
SELECT Title, related_post_count
FROM related_counts
WHERE related_post_count = (SELECT MAX(related_post_count) FROM related_counts)
ORDER BY Title;
