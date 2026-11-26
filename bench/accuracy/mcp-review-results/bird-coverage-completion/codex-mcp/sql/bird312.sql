SELECT ROUND(
    100.0 * COUNT(*) FILTER (
        WHERE major.major_id NOT IN (
            SELECT member.link_to_major
            FROM member
            WHERE member.link_to_major IS NOT NULL
        )
    ) / NULLIF(COUNT(*), 0),
    2
) AS percentage_without_members
FROM major
LIMIT 1000
